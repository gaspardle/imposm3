package sqlserver

import (
	"fmt"
)

func (mssql *Mssql) rotate(source, dest, backup string) error {
	defer log.StopStep(log.StartStep(fmt.Sprintf("Rotating tables")))

	if err := mssql.createSchema(dest); err != nil {
		return err
	}

	if err := mssql.createSchema(backup); err != nil {
		return err
	}

	tx, err := mssql.Db.Begin()
	if err != nil {
		return err
	}
	defer rollbackIfTx(&tx)

	for _, tableName := range mssql.tableNames() {
		tableName = mssql.Prefix + tableName

		log.Printf("Rotating %s from %s -> %s -> %s", tableName, source, dest, backup)

		backupExists, err := tableExists(tx, backup, tableName)
		if err != nil {
			return err
		}
		sourceExists, err := tableExists(tx, source, tableName)
		if err != nil {
			return err
		}
		destExists, err := tableExists(tx, dest, tableName)
		if err != nil {
			return err
		}

		if !sourceExists {
			log.Warnf("skipping rotate of %s, table does not exists in %s", tableName, source)
			continue
		}

		if destExists {
			log.Printf("backup of %s, to %s", tableName, backup)
			if backupExists {
				err = dropTableIfExists(tx, backup, tableName)
				if err != nil {
					return err
				}
			}
		
			sql := fmt.Sprintf(`ALTER SCHEMA "%s" TRANSFER "%s"."%s"`, backup, dest, tableName)
			_, err = tx.Exec(sql)
			if err != nil {
				return err
			}
		}

		sql := fmt.Sprintf(`ALTER SCHEMA "%s" TRANSFER "%s"."%s"`, dest, source, tableName)
		_, err = tx.Exec(sql)
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	tx = nil // set nil to prevent rollback
	return nil
}

func (mssql *Mssql) Deploy() error {
	return mssql.rotate(mssql.Config.ImportSchema, mssql.Config.ProductionSchema, mssql.Config.BackupSchema)
}

func (mssql *Mssql) RevertDeploy() error {
	return mssql.rotate(mssql.Config.BackupSchema, mssql.Config.ProductionSchema, mssql.Config.ImportSchema)
}

func (mssql *Mssql) RemoveBackup() error {
	tx, err := mssql.Db.Begin()
	if err != nil {
		return err
	}
	defer rollbackIfTx(&tx)

	backup := mssql.Config.BackupSchema

	for _, tableName := range mssql.tableNames() {
		tableName = mssql.Prefix + tableName

		backupExists, err := tableExists(tx, backup, tableName)
		if err != nil {
			return err
		}
		if backupExists {
			log.Printf("removing backup of %s from %s", tableName, backup)
			err = dropTableIfExists(tx, backup, tableName)
			if err != nil {
				return err
			}

		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	tx = nil // set nil to prevent rollback
	return nil
}

// tableNames returns a list of all tables (without prefix).
func (mssql *Mssql) tableNames() []string {
	var names []string
	for name, _ := range mssql.Tables {
		names = append(names, name)
	}
	for name, _ := range mssql.GeneralizedTables {
		names = append(names, name)
	}
	return names
}
