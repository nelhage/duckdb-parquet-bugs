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
			required fixed_len_byte_array(16) data;
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

	type record struct {
		Data [16]byte `parquet:"data"`
	}

	input := []record{
		{
			Data: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
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
