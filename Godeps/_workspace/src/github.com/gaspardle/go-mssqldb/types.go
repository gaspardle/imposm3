package mssql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"
)

// fixed-length data types
// http://msdn.microsoft.com/en-us/library/dd341171.aspx
const (
	typeNull     = 0x1f
	typeInt1     = 0x30
	typeBit      = 0x32
	typeInt2     = 0x34
	typeInt4     = 0x38
	typeDateTim4 = 0x3a
	typeFlt4     = 0x3b
	typeMoney    = 0x3c
	typeDateTime = 0x3d
	typeFlt8     = 0x3e
	typeMoney4   = 0x7a
	typeInt8     = 0x7f
)

// variable-length data types
// http://msdn.microsoft.com/en-us/library/dd358341.aspx
const (
	// byte len types
	typeGuid            = 0x24
	typeIntN            = 0x26
	typeDecimal         = 0x37 // legacy
	typeNumeric         = 0x3f // legacy
	typeBitN            = 0x68
	typeDecimalN        = 0x6a
	typeNumericN        = 0x6c
	typeFltN            = 0x6d
	typeMoneyN          = 0x6e
	typeDateTimeN       = 0x6f
	typeDateN           = 0x28
	typeTimeN           = 0x29
	typeDateTime2N      = 0x2a
	typeDateTimeOffsetN = 0x2b
	typeChar            = 0x2f // legacy
	typeVarChar         = 0x27 // legacy
	typeBinary          = 0x2d // legacy
	typeVarBinary       = 0x25 // legacy

	// short length types
	typeBigVarBin  = 0xa5
	typeBigVarChar = 0xa7
	typeBigBinary  = 0xad
	typeBigChar    = 0xaf
	typeNVarChar   = 0xe7
	typeNChar      = 0xef
	typeXml        = 0xf1
	typeUdt        = 0xf0

	// long length types
	typeText    = 0x23
	typeImage   = 0x22
	typeNText   = 0x63
	typeVariant = 0x62
)
const PLP_NULL = 0xFFFFFFFFFFFFFFFF
const UNKNOWN_PLP_LEN = 0xFFFFFFFFFFFFFFFE
const PLP_TERMINATOR = 0x00000000

// TYPE_INFO rule
// http://msdn.microsoft.com/en-us/library/dd358284.aspx
type typeInfo struct {
	TypeId    uint8
	Size      int
	Scale     uint8
	Prec      uint8
	Buffer    []byte
	Collation collation
	UdtInfo   udtInfo
	XmlInfo   xmlInfo
	Reader    func(ti *typeInfo, r *tdsBuffer) (res interface{})
	Writer    func(w io.Writer, ti typeInfo, buf []byte) (err error)
}

// Common Language Runtime (CLR) Instances
// http://msdn.microsoft.com/en-us/library/dd357962.aspx
type udtInfo struct {
	//MaxByteSize         uint32
	DBName                string
	SchemaName            string
	TypeName              string
	AssemblyQualifiedName string
}

// XML Values
// http://msdn.microsoft.com/en-us/library/dd304764.aspx
type xmlInfo struct {
	SchemaPresent       uint8
	DBName              string
	OwningSchema        string
	XmlSchemaCollection string
}

func readTypeInfo(r *tdsBuffer) (res typeInfo) {
	res.TypeId = r.byte()
	switch res.TypeId {
	case typeNull, typeInt1, typeBit, typeInt2, typeInt4, typeDateTim4,
		typeFlt4, typeMoney, typeDateTime, typeFlt8, typeMoney4, typeInt8:
		// those are fixed length types
		switch res.TypeId {
		case typeNull:
			res.Size = 0
		case typeInt1, typeBit:
			res.Size = 1
		case typeInt2:
			res.Size = 2
		case typeInt4, typeDateTim4, typeFlt4, typeMoney4:
			res.Size = 4
		case typeMoney, typeDateTime, typeFlt8, typeInt8:
			res.Size = 8
		}
		res.Reader = readFixedType
		res.Buffer = make([]byte, res.Size)
	default: // all others are VARLENTYPE
		readVarLen(&res, r)
	}
	return
}

func writeTypeInfo(w io.Writer, ti *typeInfo) (err error) {
	err = binary.Write(w, binary.LittleEndian, ti.TypeId)
	if err != nil {
		return
	}
	switch ti.TypeId {
	case typeNull, typeInt1, typeBit, typeInt2, typeInt4, typeDateTim4,
		typeFlt4, typeMoney, typeDateTime, typeFlt8, typeMoney4, typeInt8:
		// those are fixed length
		ti.Writer = writeFixedType
	default: // all others are VARLENTYPE
		err = writeVarLen(w, ti)
		if err != nil {
			return
		}
	}
	return
}

func writeVarLen(w io.Writer, ti *typeInfo) (err error) {
	switch ti.TypeId {
	case typeDateN:
		ti.Writer = writeByteLenType
	case typeTimeN, typeDateTime2N, typeDateTimeOffsetN:
		if err = binary.Write(w, binary.LittleEndian, ti.Scale); err != nil {
			return
		}
		ti.Writer = writeByteLenType
	case typeGuid, typeIntN, typeDecimal, typeNumeric,
		typeBitN, typeDecimalN, typeNumericN, typeFltN,
		typeMoneyN, typeDateTimeN, typeChar,
		typeVarChar, typeBinary, typeVarBinary:
		// byle len types
		if ti.Size > 0xff {
			panic("Invalid size for BYLELEN_TYPE")
		}
		if err = binary.Write(w, binary.LittleEndian, uint8(ti.Size)); err != nil {
			return
		}
		switch ti.TypeId {
		case typeDecimal, typeNumeric, typeDecimalN, typeNumericN:
			err = binary.Write(w, binary.LittleEndian, ti.Prec)
			if err != nil {
				return
			}
			err = binary.Write(w, binary.LittleEndian, ti.Scale)
			if err != nil {
				return
			}
		}
		ti.Writer = writeByteLenType
	case typeBigVarBin, typeBigVarChar, typeBigBinary, typeBigChar,
		typeNVarChar, typeNChar, typeXml, typeUdt:
		// short len types
		if ti.Size > 8000 || ti.Size == 0 {
			if err = binary.Write(w, binary.LittleEndian, uint16(0xffff)); err != nil {
				return
			}
			ti.Writer = writePLPType
		} else {
			if err = binary.Write(w, binary.LittleEndian, uint16(ti.Size)); err != nil {
				return
			}
			ti.Writer = writeShortLenType
		}
		switch ti.TypeId {
		case typeBigVarChar, typeBigChar, typeNVarChar, typeNChar:
			if err = writeCollation(w, ti.Collation); err != nil {
				return
			}
		case typeXml:
			if err = binary.Write(w, binary.LittleEndian, ti.XmlInfo.SchemaPresent); err != nil {
				return
			}
		}
	case typeText, typeImage, typeNText, typeVariant:
		// LONGLEN_TYPE
		if err = binary.Write(w, binary.LittleEndian, uint32(ti.Size)); err != nil {
			return
		}
		if err = writeCollation(w, ti.Collation); err != nil {
			return
		}
		ti.Writer = writeLongLenType
	default:
		panic("Invalid type")
	}
	return
}

// http://msdn.microsoft.com/en-us/library/ee780895.aspx
func decodeDateTim4(buf []byte) time.Time {
	days := binary.LittleEndian.Uint16(buf)
	mins := binary.LittleEndian.Uint16(buf[2:])
	return time.Date(1900, 1, 1+int(days),
		0, int(mins), 0, 0, time.UTC)
}

func decodeDateTime(buf []byte) time.Time {
	days := int32(binary.LittleEndian.Uint32(buf))
	tm := binary.LittleEndian.Uint32(buf[4:])
	ns := int(math.Trunc(float64(tm%300)/0.3+0.5)) * 1000000
	secs := int(tm / 300)
	return time.Date(1900, 1, 1+int(days),
		0, 0, secs, ns, time.UTC)
}

func readFixedType(ti *typeInfo, r *tdsBuffer) (res interface{}) {
	r.ReadFull(ti.Buffer)
	buf := ti.Buffer
	switch ti.TypeId {
	case typeNull:
		return nil
	case typeInt1:
		return int64(buf[0])
	case typeBit:
		return buf[0] != 0
	case typeInt2:
		return int64(int16(binary.LittleEndian.Uint16(buf)))
	case typeInt4:
		return int64(int32(binary.LittleEndian.Uint32(buf)))
	case typeDateTim4:
		return decodeDateTim4(buf)
	case typeFlt4:
		return math.Float32frombits(binary.LittleEndian.Uint32(buf))
	case typeMoney4:
		return decodeMoney4(buf)
	case typeMoney:
		return decodeMoney(buf)
	case typeDateTime:
		return decodeDateTime(buf)
	case typeFlt8:
		return math.Float64frombits(binary.LittleEndian.Uint64(buf))
	case typeInt8:
		return int64(binary.LittleEndian.Uint64(buf))
	default:
		badStreamPanicf("Invalid typeid")
	}
	panic("shoulnd't get here")
}

func writeFixedType(w io.Writer, ti typeInfo, buf []byte) (err error) {
	_, err = w.Write(buf)
	return
}

func readByteLenType(ti *typeInfo, r *tdsBuffer) (res interface{}) {
	size := r.byte()
	if size == 0 {
		return nil
	}
	r.ReadFull(ti.Buffer[:size])
	buf := ti.Buffer[:size]
	switch ti.TypeId {
	case typeDateN:
		if len(buf) != 3 {
			badStreamPanicf("Invalid size for DATENTYPE")
		}
		return decodeDate(buf)
	case typeTimeN:
		return decodeTime(ti.Scale, buf)
	case typeDateTime2N:
		return decodeDateTime2(ti.Scale, buf)
	case typeDateTimeOffsetN:
		return decodeDateTimeOffset(ti.Scale, buf)
	case typeGuid:
		return decodeGuid(buf)
	case typeIntN:
		switch len(buf) {
		case 1:
			return int64(buf[0])
		case 2:
			return int64(int16((binary.LittleEndian.Uint16(buf))))
		case 4:
			return int64(int32(binary.LittleEndian.Uint32(buf)))
		case 8:
			return int64(binary.LittleEndian.Uint64(buf))
		default:
			badStreamPanicf("Invalid size for INTNTYPE")
		}
	case typeDecimal, typeNumeric, typeDecimalN, typeNumericN:
		return decodeDecimal(ti.Prec, ti.Scale, buf)
	case typeBitN:
		if len(buf) != 1 {
			badStreamPanicf("Invalid size for BITNTYPE")
		}
		return buf[0] != 0
	case typeFltN:
		switch len(buf) {
		case 4:
			return float64(math.Float32frombits(binary.LittleEndian.Uint32(buf)))
		case 8:
			return math.Float64frombits(binary.LittleEndian.Uint64(buf))
		default:
			badStreamPanicf("Invalid size for FLTNTYPE")
		}
	case typeMoneyN:
		switch len(buf) {
		case 4:
			return decodeMoney4(buf)
		case 8:
			return decodeMoney(buf)
		default:
			badStreamPanicf("Invalid size for MONEYNTYPE")
		}
	case typeDateTimeN:
		switch len(buf) {
		case 4:
			return decodeDateTim4(buf)
		case 8:
			return decodeDateTime(buf)
		default:
			badStreamPanicf("Invalid size for DATETIMENTYPE")
		}
	case typeChar, typeVarChar:
		return decodeChar(ti.Collation, buf)
	case typeBinary, typeVarBinary:
		// a copy, because the backing array for ti.Buffer is reused
		// and can be overwritten by the next row while this row waits
		// in a buffered chan
		cpy := make([]byte, len(buf))
		copy(cpy, buf)
		return cpy
	default:
		badStreamPanicf("Invalid typeid")
	}
	panic("shoulnd't get here")
}

func writeByteLenType(w io.Writer, ti typeInfo, buf []byte) (err error) {
	if ti.Size > 0xff {
		panic("Invalid size for BYTELEN_TYPE")
	}
	err = binary.Write(w, binary.LittleEndian, uint8(ti.Size))
	if err != nil {
		return
	}
	_, err = w.Write(buf)
	return
}

func readShortLenType(ti *typeInfo, r *tdsBuffer) (res interface{}) {
	size := r.uint16()
	if size == 0xffff {
		return nil
	}
	r.ReadFull(ti.Buffer[:size])
	buf := ti.Buffer[:size]
	switch ti.TypeId {
	case typeBigVarChar, typeBigChar:
		return decodeChar(ti.Collation, buf)
	case typeBigVarBin, typeBigBinary:
		// a copy, because the backing array for ti.Buffer is reused
		// and can be overwritten by the next row while this row waits
		// in a buffered chan
		cpy := make([]byte, len(buf))
		copy(cpy, buf)
		return cpy
	case typeNVarChar, typeNChar:
		return decodeNChar(buf)
	case typeUdt:
		return decodeUdt(*ti, buf)
	default:
		badStreamPanicf("Invalid typeid")
	}
	panic("shoulnd't get here")
}

func writeShortLenType(w io.Writer, ti typeInfo, buf []byte) (err error) {
	if buf == nil {
		err = binary.Write(w, binary.LittleEndian, uint16(0xffff))
		return
	}
	if ti.Size > 0xfffe {
		panic("Invalid size for USHORTLEN_TYPE")
	}
	err = binary.Write(w, binary.LittleEndian, uint16(ti.Size))
	if err != nil {
		return
	}
	_, err = w.Write(buf)
	return
}

func readLongLenType(ti *typeInfo, r *tdsBuffer) (res interface{}) {
	// information about this format can be found here:
	// http://msdn.microsoft.com/en-us/library/dd304783.aspx
	// and here:
	// http://msdn.microsoft.com/en-us/library/dd357254.aspx
	textptrsize := r.byte()
	if textptrsize == 0 {
		return nil
	}
	textptr := make([]byte, textptrsize)
	r.ReadFull(textptr)
	timestamp := r.uint64()
	_ = timestamp // ignore timestamp
	size := r.int32()
	if size == -1 {
		return nil
	}
	buf := make([]byte, size)
	r.ReadFull(buf)
	switch ti.TypeId {
	case typeText:
		return decodeChar(ti.Collation, buf)
	case typeImage:
		return buf
	case typeNText:
		return decodeNChar(buf)
	default:
		badStreamPanicf("Invalid typeid")
	}
	panic("shoulnd't get here")
}
func writeLongLenType(w io.Writer, ti typeInfo, buf []byte) (err error) {
	//textptr
	err = binary.Write(w, binary.LittleEndian, byte(0x10))
	if err != nil {
		return
	}
	err = binary.Write(w, binary.LittleEndian, uint64(0xFFFFFFFFFFFFFFFF))
	if err != nil {
		return
	}
	err = binary.Write(w, binary.LittleEndian, uint64(0xFFFFFFFFFFFFFFFF))
	if err != nil {
		return
	}
	//timestamp?
	err = binary.Write(w, binary.LittleEndian, uint64(0xFFFFFFFFFFFFFFFF))
	if err != nil {
		return
	}

	err = binary.Write(w, binary.LittleEndian, uint32(ti.Size))
	if err != nil {
		return
	}
	_, err = w.Write(buf)
	return
}

// reads variant value
// http://msdn.microsoft.com/en-us/library/dd303302.aspx
func readVariantType(ti *typeInfo, r *tdsBuffer) (res interface{}) {
	size := r.int32()
	if size == 0 {
		return nil
	}
	vartype := r.byte()
	propbytes := int32(r.byte())
	switch vartype {
	case typeGuid:
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return buf
	case typeBit:
		return r.byte() != 0
	case typeInt1:
		return int64(r.byte())
	case typeInt2:
		return int64(int16(r.uint16()))
	case typeInt4:
		return int64(r.int32())
	case typeInt8:
		return int64(r.uint64())
	case typeDateTime:
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return decodeDateTime(buf)
	case typeDateTim4:
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return decodeDateTim4(buf)
	case typeFlt4:
		return float64(math.Float32frombits(r.uint32()))
	case typeFlt8:
		return math.Float64frombits(r.uint64())
	case typeMoney4:
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return decodeMoney4(buf)
	case typeMoney:
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return decodeMoney(buf)
	case typeDateN:
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return decodeDate(buf)
	case typeTimeN:
		scale := r.byte()
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return decodeTime(scale, buf)
	case typeDateTime2N:
		scale := r.byte()
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return decodeDateTime2(scale, buf)
	case typeDateTimeOffsetN:
		scale := r.byte()
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return decodeDateTimeOffset(scale, buf)
	case typeBigVarBin, typeBigBinary:
		r.uint16() // max length, ignoring
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return buf
	case typeDecimalN, typeNumericN:
		prec := r.byte()
		scale := r.byte()
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return decodeDecimal(prec, scale, buf)
	case typeBigVarChar, typeBigChar:
		col := readCollation(r)
		r.uint16() // max length, ignoring
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return decodeChar(col, buf)
	case typeNVarChar, typeNChar:
		_ = readCollation(r)
		r.uint16() // max length, ignoring
		buf := make([]byte, size-2-propbytes)
		r.ReadFull(buf)
		return decodeNChar(buf)
	default:
		badStreamPanicf("Invalid variant typeid")
	}
	panic("shoulnd't get here")
}

// partially length prefixed stream
// http://msdn.microsoft.com/en-us/library/dd340469.aspx
func readPLPType(ti *typeInfo, r *tdsBuffer) (res interface{}) {
	size := r.uint64()
	var buf *bytes.Buffer
	switch size {
	case PLP_NULL:
		// null
		return nil
	case UNKNOWN_PLP_LEN:
		// size unknown
		buf = bytes.NewBuffer(make([]byte, 0, 1000))
	default:
		buf = bytes.NewBuffer(make([]byte, 0, size))
	}
	for true {
		chunksize := r.uint32()
		if chunksize == 0 {
			break
		}
		if _, err := io.CopyN(buf, r, int64(chunksize)); err != nil {
			badStreamPanicf("Reading PLP type failed: %s", err.Error())
		}
	}
	switch ti.TypeId {
	case typeXml:
		return decodeXml(*ti, buf.Bytes())
	case typeBigVarChar, typeBigChar, typeText:
		return decodeChar(ti.Collation, buf.Bytes())
	case typeBigVarBin, typeBigBinary, typeImage:
		return buf.Bytes()
	case typeNVarChar, typeNChar, typeNText:
		return decodeNChar(buf.Bytes())
	case typeUdt:
		return decodeUdt(*ti, buf.Bytes())
	}
	panic("shoulnd't get here")
}

func writePLPType(w io.Writer, ti typeInfo, buf []byte) (err error) {

	if len(buf) == 0 {
		err = binary.Write(w, binary.LittleEndian, uint64(PLP_NULL))
		return
	}

	if err = binary.Write(w, binary.LittleEndian, uint64(UNKNOWN_PLP_LEN)); err != nil {
		return
	}
	for {
		chunksize := uint32(len(buf))
		if chunksize == 0 {
			err = binary.Write(w, binary.LittleEndian, uint32(PLP_TERMINATOR))
			return
		}
		if err = binary.Write(w, binary.LittleEndian, chunksize); err != nil {
			return
		}
		if _, err = w.Write(buf[:chunksize]); err != nil {
			return
		}
		buf = buf[chunksize:]
	}
}

func readVarLen(ti *typeInfo, r *tdsBuffer) {
	switch ti.TypeId {
	case typeDateN:
		ti.Size = 3
		ti.Reader = readByteLenType
		ti.Buffer = make([]byte, ti.Size)
	case typeTimeN, typeDateTime2N, typeDateTimeOffsetN:
		ti.Scale = r.byte()
		switch ti.Scale {
		case 0, 1, 2:
			ti.Size = 3
		case 3, 4:
			ti.Size = 4
		case 5, 6, 7:
			ti.Size = 5
		default:
			badStreamPanicf("Invalid scale for TIME/DATETIME2/DATETIMEOFFSET type")
		}
		switch ti.TypeId {
		case typeDateTime2N:
			ti.Size += 3
		case typeDateTimeOffsetN:
			ti.Size += 5
		}
		ti.Reader = readByteLenType
		ti.Buffer = make([]byte, ti.Size)
	case typeGuid, typeIntN, typeDecimal, typeNumeric,
		typeBitN, typeDecimalN, typeNumericN, typeFltN,
		typeMoneyN, typeDateTimeN, typeChar,
		typeVarChar, typeBinary, typeVarBinary:
		// byle len types
		ti.Size = int(r.byte())
		ti.Buffer = make([]byte, ti.Size)
		switch ti.TypeId {
		case typeDecimal, typeNumeric, typeDecimalN, typeNumericN:
			ti.Prec = r.byte()
			ti.Scale = r.byte()
		}
		ti.Reader = readByteLenType
	case typeXml:
		ti.XmlInfo.SchemaPresent = r.byte()
		if ti.XmlInfo.SchemaPresent != 0 {
			// dbname
			ti.XmlInfo.DBName = r.BVarChar()
			// owning schema
			ti.XmlInfo.OwningSchema = r.BVarChar()
			// xml schema collection
			ti.XmlInfo.XmlSchemaCollection = r.UsVarChar()
		}
		ti.Reader = readPLPType
	case typeUdt:
		ti.Size = int(r.uint16())
		ti.UdtInfo.DBName = r.BVarChar()
		ti.UdtInfo.SchemaName = r.BVarChar()
		ti.UdtInfo.TypeName = r.BVarChar()
		ti.UdtInfo.AssemblyQualifiedName = r.UsVarChar()

		ti.Buffer = make([]byte, ti.Size)
		ti.Reader = readPLPType
	case typeBigVarBin, typeBigVarChar, typeBigBinary, typeBigChar,
		typeNVarChar, typeNChar:
		// short len types
		ti.Size = int(r.uint16())
		switch ti.TypeId {
		case typeBigVarChar, typeBigChar, typeNVarChar, typeNChar:
			ti.Collation = readCollation(r)
		}
		if ti.Size == 0xffff {
			ti.Reader = readPLPType
		} else {
			ti.Buffer = make([]byte, ti.Size)
			ti.Reader = readShortLenType
		}
	case typeText, typeImage, typeNText, typeVariant:
		// LONGLEN_TYPE
		ti.Size = int(r.int32())
		switch ti.TypeId {
		case typeText, typeNText:
			ti.Collation = readCollation(r)
			// ignore tablenames
			numparts := int(r.byte())
			for i := 0; i < numparts; i++ {
				r.UsVarChar()
			}
			ti.Reader = readLongLenType
		case typeImage:
			// ignore tablenames
			numparts := int(r.byte())
			for i := 0; i < numparts; i++ {
				r.UsVarChar()
			}
			ti.Reader = readLongLenType
		case typeXml:
			panic("XMLTYPE not implemented")
		case typeVariant:
			ti.Reader = readVariantType
		}
	default:
		badStreamPanicf("Invalid type %d", ti.TypeId)
	}
	return
}

func decodeMoney(buf []byte) []byte {
	money := int64(uint64(buf[4]) |
		uint64(buf[5])<<8 |
		uint64(buf[6])<<16 |
		uint64(buf[7])<<24 |
		uint64(buf[0])<<32 |
		uint64(buf[1])<<40 |
		uint64(buf[2])<<48 |
		uint64(buf[3])<<56)
	return scaleBytes(strconv.FormatInt(money, 10), 4)
}

func decodeMoney4(buf []byte) []byte {
	money := int32(binary.LittleEndian.Uint32(buf[0:4]))
	return scaleBytes(strconv.FormatInt(int64(money), 10), 4)
}

func decodeGuid(buf []byte) []byte {
	res := make([]byte, 16)
	copy(res, buf)
	return res
}

func decodeDecimal(prec uint8, scale uint8, buf []byte) []byte {
	var sign uint8
	sign = buf[0]
	dec := Decimal{
		positive: sign != 0,
		prec:     prec,
		scale:    scale,
	}
	buf = buf[1:]
	l := len(buf) / 4
	for i := 0; i < l; i++ {
		dec.integer[i] = binary.LittleEndian.Uint32(buf[0:4])
		buf = buf[4:]
	}
	return dec.Bytes()
}

// http://msdn.microsoft.com/en-us/library/ee780895.aspx
func decodeDateInt(buf []byte) (days int) {
	return int(buf[0]) + int(buf[1])*256 + int(buf[2])*256*256
}

func decodeDate(buf []byte) time.Time {
	return time.Date(1, 1, 1+decodeDateInt(buf), 0, 0, 0, 0, time.UTC)
}

func decodeTimeInt(scale uint8, buf []byte) (sec int, ns int) {
	var acc uint64 = 0
	for i := len(buf) - 1; i >= 0; i-- {
		acc <<= 8
		acc |= uint64(buf[i])
	}
	for i := 0; i < 7-int(scale); i++ {
		acc *= 10
	}
	nsbig := acc * 100
	sec = int(nsbig / 1000000000)
	ns = int(nsbig % 1000000000)
	return
}

func decodeTime(scale uint8, buf []byte) time.Time {
	sec, ns := decodeTimeInt(scale, buf)
	return time.Date(1, 1, 1, 0, 0, sec, ns, time.UTC)
}

func decodeDateTime2(scale uint8, buf []byte) time.Time {
	timesize := len(buf) - 3
	sec, ns := decodeTimeInt(scale, buf[:timesize])
	days := decodeDateInt(buf[timesize:])
	return time.Date(1, 1, 1+days, 0, 0, sec, ns, time.UTC)
}

func decodeDateTimeOffset(scale uint8, buf []byte) time.Time {
	timesize := len(buf) - 3 - 2
	sec, ns := decodeTimeInt(scale, buf[:timesize])
	buf = buf[timesize:]
	days := decodeDateInt(buf[:3])
	buf = buf[3:]
	offset := int(int16(binary.LittleEndian.Uint16(buf))) // in mins
	return time.Date(1, 1, 1+days, 0, 0, sec+offset*60, ns,
		time.FixedZone("", offset*60))
}

func divFloor(x int64, y int64) int64 {
	q := x / y
	r := x % y
	if r != 0 && ((r < 0) != (y < 0)) {
		q--
	}
	return q
}

func dateTime2(t time.Time) (days int32, ns int64) {
	// number of days since Jan 1 1970 UTC
	days64 := divFloor(t.Unix(), 24*60*60)
	// number of days since Jan 1 1 UTC
	days = int32(days64) + 1969*365 + 1969/4 - 1969/100 + 1969/400
	// number of seconds within day
	secs := t.Unix() - days64*24*60*60
	// number of nanoseconds within day
	ns = secs*1e9 + int64(t.Nanosecond())
	return
}

func decodeChar(col collation, buf []byte) string {
	return charset2utf8(col, buf)
}

func decodeUcs2(buf []byte) string {
	res, err := ucs22str(buf)
	if err != nil {
		badStreamPanicf("Invalid UCS2 encoding: %s", err.Error())
	}
	return res
}

func decodeNChar(buf []byte) string {
	return decodeUcs2(buf)
}

func decodeXml(ti typeInfo, buf []byte) string {
	return decodeUcs2(buf)
}

func decodeUdt(ti typeInfo, buf []byte) []byte {
	return buf
}

func makeDecl(ti typeInfo) string {
	switch ti.TypeId {
	case typeInt1:
		return "tinyint"
	case typeInt2:
		return "smallint"
	case typeInt4:
		return "int"
	case typeInt8:
		return "bigint"
	case typeFlt4:
		return "real"
	case typeIntN:
		switch ti.Size {
		case 1:
			return "tinyint"
		case 2:
			return "smallint"
		case 4:
			return "int"
		case 8:
			return "bigint"
		default:
			panic("invalid size of INTNTYPE")
		}
	case typeFlt8:
		return "float"
	case typeFltN:
		switch ti.Size {
		case 4:
			return "real"
		case 8:
			return "float"
		default:
			panic("invalid size of FLNNTYPE")
		}
	case typeDecimal, typeDecimalN:
		return fmt.Sprintf("decimal(%d, %d)", ti.Prec, ti.Scale)
	case typeMoney4:
		return "smallmoney"
	case typeMoney:
		return "money"
	case typeMoneyN:
		switch ti.Size {
		case 4:
			return "smallmoney"
		case 8:
			return "money"
		default:
			panic("invalid size of MONEYNTYPE")
		}
	case typeBigVarBin:
		if ti.Size > 8000 || ti.Size == 0 {
			return fmt.Sprintf("varbinary(max)")
		} else {
			return fmt.Sprintf("varbinary(%d)", ti.Size)
		}
	case typeNChar:
		return fmt.Sprintf("nchar(%d)", ti.Size/2)
	case typeBigChar, typeChar:
		return fmt.Sprintf("char(%d)", ti.Size)
	case typeBigVarChar, typeVarChar:
		if ti.Size > 4000 || ti.Size == 0 {
			return fmt.Sprintf("varchar(max)")
		} else {
			return fmt.Sprintf("varchar(%d)", ti.Size)
		}
	case typeNVarChar:
		if ti.Size > 8000 || ti.Size == 0 {
			return fmt.Sprintf("nvarchar(max)")
		} else {
			return fmt.Sprintf("nvarchar(%d)", ti.Size/2)
		}
	case typeBit, typeBitN:
		return "bit"
	case typeDateTim4:
		return "smalldatetime"
	case typeDateN:
		return "date"
	case typeDateTime:
		return "datetime"
	case typeDateTimeN:
		switch ti.Size {
		case 4:
			return "smalldatetime"
		case 8:
			return "datetime"
		default:
			panic("invalid size of DATETIMNTYPE")
		}
	case typeDateTime2N:
		return fmt.Sprintf("datetime2(%d)", ti.Scale)
	case typeDateTimeOffsetN:
		return fmt.Sprintf("datetimeoffset(%d)", ti.Scale)
	case typeText:
		return "text"
	case typeNText:
		return "ntext"
	case typeUdt:
		return ti.UdtInfo.TypeName
	default:
		panic(fmt.Sprintf("not implemented makeDecl for type %#x", ti.TypeId))
	}
}
