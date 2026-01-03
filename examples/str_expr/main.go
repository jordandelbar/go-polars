package main

import (
	"fmt"

	pl "github.com/jordandelbar/go-polars/polars"
)

func main() {
	irisDf, err := pl.NewDataFrame().
		AddStringColumn("variety", []string{
			"Setosa",
			"Versicolor",
			"Virginica",
			"Setosa",
			"Versicolor",
			"Virginica",
		}).
		Build()
	if err != nil {
		panic(err)
	}

	stringOps := irisDf.WithColumns(
		pl.Col("variety").StrToLower().Alias("variety_lower"),
		pl.Col("variety").StrContains("(?i)setosa", false).Alias("contains_setosa_regex"),
		pl.Col("variety").StrStartsWith("Ver").Alias("starts_ver"),
		pl.Col("variety").StrEndsWith("ica").Alias("ends_ica"),
		pl.Col("variety").StrLenChars().Alias("name_len"),
		pl.Col("variety").StrReplace("(gin)ica$", "-$1-", false).Alias("variety_replaced_regex"),
		pl.Col("variety").StrSlice(0, 3).Alias("variety_prefix"),
		pl.Col("variety").StrStripChars("V").Alias("variety_strip_v"),
		pl.Col("variety").StrExtract("(Set)(osa)", 1).Alias("extract_group1"),
		pl.Col("variety").StrSplit(" ").Alias("split_space"),
		pl.Col("variety").StrSplitInclusive("os").Alias("split_incl_os"),
	)

	// is_in over string values
	isInFiltered := irisDf.Filter(pl.Col("variety").IsIn([]string{"Setosa", "Versicolor"}))

	fmt.Println("String helpers demo (first 5 rows):")
	fmt.Println(stringOps.Head(5))

	fmt.Println("\nis_in demo (first 5 rows):")
	fmt.Println(isInFiltered.Head(5))
}
