package main

import (
	"log"
	"os"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/floor"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

func main() {
	schemaDef, err := parquetschema.ParseSchemaDefinition(
		`message test {
			required group inner {
				optional binary str_field (STRING);
				optional double f64_field;
			}
		}`)
	if err != nil {
		log.Fatalf("Parsing schema definition failed: %v", err)
	}

	parquetFilename := os.Args[1]

	fw, err := floor.NewFileWriter(parquetFilename,
		goparquet.WithSchemaDefinition(schemaDef),
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
	)
	if err != nil {
		log.Fatalf("Opening parquet file for writing failed: %v", err)
	}

	type inner struct {
		StrField *string  `parquet:"str_field"`
		F64Field *float64 `parquet:"f64_field"`
	}
	type record struct {
		Inner inner `parquet: "inner"`
	}

	str := "hello"
	f64 := float64(1.23)

	input := []record{
		{
			Inner: inner{
				StrField: &str,
			},
		},
		{
			Inner: inner{
				F64Field: &f64,
			},
		},
	}

	for _, rec := range input {
		if err := fw.Write(rec); err != nil {
			log.Fatalf("Writing record failed: %v", err)
		}
	}

	if err := fw.Close(); err != nil {
		log.Fatalf("Closing parquet writer failed: %v", err)
	}
}
