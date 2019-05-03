package main

import (
	"flag"
	"os"
	"log"
	"path/filepath"
	"strings"
	"io"
	"text/template"

	"github.com/knakk/rdf"
	"sync"
	"encoding/json"
)

type TemplateVars struct {
	Subject string
	Object  string
	Field   string
}

var predicateFlag = flag.String("predicate", "", "rdf predicate to match")
var fieldFlag = flag.String("field", "", "name of the field in solr to store the value")
var templateString = flag.String(
	"template",
	`{"id":{{.Subject}},"{{.Field}}":{"add":{{.Object}}}}`,
	`go template (text/template) to print as a line in the json command. 
Available vars are: {{.Subject}} {{.Object}} and {{.Field}}
{{.Subject}} and {{.Object}} are escaped through json.Marshal, so the template should NOT include quotes around these fields`,
)

func main() {
	flag.Parse()
	predicate, predErr := rdf.NewIRI(*predicateFlag)
	if predErr != nil {
		log.Fatal(predErr)
	}
	log.Println("Using predicate: " + predicate.String())
	tmpl, templateErr := template.New("line").Parse(*templateString)
	if templateErr != nil {
		log.Fatal(templateErr)
	}
	var masterWg sync.WaitGroup
	for _, f := range flag.Args() {
		if file, err := os.Open(f); err == nil {
			masterWg.Add(1)
			go func(wg *sync.WaitGroup, tripleFile *os.File) {
				defer wg.Done()
				defer file.Close()
				var format rdf.Format
				switch filepath.Ext(file.Name()) {
				case "ttl":
					log.Println("Got Turtle")
					format = rdf.Turtle
				case "nt":
					log.Println("Got NT")
					format = rdf.NTriples
				case "xml":
					log.Println("Got RDF/XML")
					format = rdf.RDFXML
				}
				decoder := rdf.NewTripleDecoder(tripleFile, format)
				name := strings.TrimSuffix(tripleFile.Name(), filepath.Ext(tripleFile.Name()))
				outFile, fileErr := os.OpenFile(name+".json", os.O_RDWR|os.O_CREATE, 0644)
				if fileErr != nil {
					log.Printf("Error opening output file %s, ignoring: %v", name+".json", fileErr)
					return
				}
				outFile.WriteString("[")
				defer outFile.Close()
				defer outFile.WriteString("\n")
				defer outFile.WriteString("]")
				for trip, err := decoder.Decode(); err != io.EOF; trip, err = decoder.Decode() {
					if err != nil {
						log.Print(err)
						continue
					}
					// Do json escaping
					s, _ := json.Marshal(trip.Subj.String())
					o, _ := json.Marshal(trip.Obj.String())
					item := &TemplateVars{
						Subject: string(s),
						Field:   *fieldFlag,
						Object:  string(o),
					}
					if rdf.TermsEqual(trip.Pred, predicate) {
						tmpl.Execute(outFile, item)
						outFile.WriteString(",\n")
					}
				}
				log.Println("Finished reading file: " + tripleFile.Name())
			}(&masterWg, file)
		} else {
			log.Printf("Error opening input file %s, ignoring: %v", f, err)
			continue
		}
	}
	masterWg.Wait()
}
