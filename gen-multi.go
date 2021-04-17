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
	type File struct {
		Filename string
		Schema   string
		Data     []interface{}
	}
	type Record struct {
		Str   string  `parquet:"str"`
		Float float64 `parquet:"float"`
		Int   int64   `parquet:"int"`
	}
	files := []File{
		{
			Filename: os.Args[1],
			Schema: `
message test {
  required binary str (STRING);
  required double float;
}
			`,
			Data: []interface{}{
				Record{
					Str:   "a",
					Float: 1.0,
				},
				Record{
					Str:   "a",
					Float: 2.5,
				},
			},
		},
		{
			Filename: os.Args[2],
			Schema: `
message test {
  required int64 int;
  required binary str (STRING);
}
			`,
			Data: []interface{}{
				Record{
					Str: "b",
					Int: 1,
				},
				Record{
					Str: "b",
					Int: 2,
				},
				Record{
					Str: "b",
					Int: 3,
				},
			},
		},
	}

	for _, f := range files {
		schemaDef, err := parquetschema.ParseSchemaDefinition(f.Schema)
		if err != nil {
			log.Fatalf("Parsing schema definition failed: %v", err)
		}

		fw, err := floor.NewFileWriter(f.Filename,
			goparquet.WithSchemaDefinition(schemaDef),
			goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		)
		if err != nil {
			log.Fatalf("Opening parquet file for writing failed: %v", err)
		}

		for _, rec := range f.Data {
			if err := fw.Write(rec); err != nil {
				log.Fatalf("Writing record failed: %v", err)
			}
		}

		if err := fw.Close(); err != nil {
			log.Fatalf("Closing parquet writer failed: %v", err)
		}
	}
}
