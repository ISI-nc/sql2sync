package db

import "fmt"

type TableInfo struct {
	Name        string
	KeyColumns  []ColumnInfo
	DataColumns []ColumnInfo
}

type ColumnInfo struct {
	Name       string
	Conversion string
}

func (i *TableInfo) OverrideKeyColumns(keyOverrides []string) error {
	for _, col := range keyOverrides {
		found := false
		for idx, colInfo := range i.DataColumns {
			if colInfo.Name != col {
				continue
			}
			found = true
			i.KeyColumns = append(i.KeyColumns, colInfo)
			i.DataColumns = append(i.DataColumns[:idx], i.DataColumns[idx+1:]...)
			break
		}
		if !found {
			return fmt.Errorf("Column %s not found in DataColumns", col)
		}
	}
	return nil
}
