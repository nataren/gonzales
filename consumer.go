package main

// Send a bunch of messages to AWS Kinesis
import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"log"
	"os"
	"runtime"
)

func main() {
	cpus := runtime.NumCPU()
	runtime.GOMAXPROCS(cpus)

	// Deserialize the Kinesis blob
	var blob KinesisBlob
	jsonErr := json.Unmarshal([]byte(os.Args[1]), &blob)
	if jsonErr != nil {
		log.Fatal(jsonErr)
	}

	// Deserialize the Kinesis records
	for _, record := range blob.Records {
		log.Printf("Kinesis record: %v", record)
		ev := Event{}
		data, decodingErr := base64.StdEncoding.DecodeString(record.Value.Data)
		if decodingErr != nil {
			log.Fatal(decodingErr)
		}
		log.Printf("Decoded data to: %v", data)
		xmlErr := xml.Unmarshal(data, &ev)
		if xmlErr != nil {
			log.Printf("Could not deserialize: '%v'", xmlErr)
		} else {
			log.Printf("Deserialized: '%v'", ev)
		}
		if ev.Any != "" {
			log.Printf("'The event '%s' was found not having all its members deserialized. Any = '%s'", ev, ev.Any)
		}
	}
}

type KinesisBlob struct {
	Records []KinesisRecord `json:"Records"`
}

type KinesisRecord struct {
	Value KinesisValue `json:"kinesis"`
}

type KinesisValue struct {
	PartitionKey         string `json:"partitionKey"`
	KinesisSchemaVersion string `json:"kinesisSchemaVersion"`
	Data                 string `json:"data"`
	SequenceNumber       string `json:"sequenceNumber"`
}

type Event struct {
	XMLName               xml.Name       `xml:"event"`
	ID                    string         `xml:"id,attr"`
	Datetime              string         `xml:"datetime,attr"`
	Type                  string         `xml:"type,attr"`
	Cascading             string         `xml:"cascading,attr"`
	Wikiid                string         `xml:"wikiid,attr"`
	Journaled             string         `xml:"journaled,attr"`
	Version               string         `xml:"version,attr"`
	Request               Request        `xml:"request"`
	IsImage               string         `xml:"isimage"`
	Page                  Page           `xml:"page"`
	File                  File           `xml:"file"`
	Data                  Data           `xml:"data"`
	Diff                  Diff           `xml:"diff"`
	CreateReason          string         `xml:"create-reason"`
	User                  User           `xml:"user"`
	CreateReasonDetail    string         `xml:"create-reason-detail"`
	DescendantPage        DescendantPage `xml:"descendant.page"`
	RootCopyPage          RootCopyPage   `xml:"root.copy.page"`
	RootDeletePage        RootDeletePage `xml:"root.delete.page"`
	RootPage              RootPage       `xml:"root.page"`
	SourcePage            SourcePage     `xml:"source.page"`
	From                  string         `xml:"from"`
	To                    string         `xml:"to"`
	Revision              string         `xml:"revision"`
	RevisionPrevious      string         `xml:"revision.previous"`
	RevisionReverted      string         `xml:"revision.reverted"`
	Comment               Comment        `xml:"comment"`
	TagsAdded             []Tag          `xml:"tags-added>tag"`
	TagsRemoved           []Tag          `xml:"tags-removed>tag"`
	Property              Property       `xml:"property"`
	RestrictionID         string         `xml:"restriction-id"`
	PreviousRestrictionID string         `xml:"previous.restriction-id"`
	Score                 string         `xml:"score"`
	Grant                 Grant          `xml:"grant"`
	Any                   string         `xml:",any"`
	AuthMethodPassword    string         `xml:"authmethod-password"`
	AuthMethodApikey      string         `xml:"authmethod-apikey"`
	Origin                string         `xml:"origin"`
	ReasonType            string         `xml:"reason-type"`
	DisplayNamePrevious   string         `xml:"displayname.previous"`
	DisplayNameCurrent    string         `xml:"displayname.current"`
	ContentTypePrevious   string         `xml:"contenttype.previous"`
	ContentTypeCurrent    string         `xml:"contenttype.current"`
	ChangeComment         string         `xml:"change-comment"`
	TitleSegmentPrevious  string         `xml:"titlesegment.previous"`
	TitleSegmentCurrent   string         `xml:"titlesegment.current"`
	Workflow              Workflow       `xml:"workflow"`
}

type Signature struct {
	XMLName xml.Name `xml:"signature"`
	Value   string   `xml:",innerxml"`
}

type IP struct {
	XMLName xml.Name `xml:"ip"`
	Value   string   `xml:",innerxml"`
}

type SessionID struct {
	XMLName xml.Name `xml:"session-id"`
	Value   string   `xml:",innerxml"`
}

type User struct {
	XMLName   xml.Name `xml:"user"`
	ID        string   `xml:"id,attr"`
	Anonymous string   `xml:"anonymous,attr"`
	Name      string   `xml:"name"`
	Username  string   `xml:"username,attr"`
}

type Parameter struct {
	XMLName xml.Name `xml:"param"`
	Name    string   `xml:"name,attr"`
	Value   string   `xml:",innerxml"`
}

type Request struct {
	XMLName    xml.Name    `xml:"request"`
	ID         string      `xml:"id,attr"`
	Seq        string      `xml:"seq,attr"`
	Count      string      `xml:"count,attr"`
	Signature  Signature   `xml:"signature"`
	IP         IP          `xml:"ip"`
	SessionID  SessionID   `xml:"session-id"`
	Parameters []Parameter `xml:"parameters>param"`
	User       User        `xml:"user"`
}

type PagePath struct {
	XMLName xml.Name `xml:"path"`
	Value   string   `xml:",innerxml"`
}

type Page struct {
	XMLName xml.Name `xml:"page"`
	ID      string   `xml:"id,attr"`
	Path    PagePath `xml:"path"`
}

type RootPage struct {
	XMLName xml.Name `xml:"root.page"`
	ID      string   `xml:"id,attr"`
	Path    PagePath `xml:"path"`
}

type SourcePage struct {
	XMLName xml.Name `xml:"source.page"`
	ID      string   `xml:"id,attr"`
	Path    PagePath `xml:"path"`
}

type DescendantPage struct {
	XMLName xml.Name `xml:"descendant.page"`
	ID      string   `xml:"id,attr"`
	Path    PagePath `xml:"path"`
}

type RootCopyPage struct {
	XMLName xml.Name `xml:"root.copy.page"`
	ID      string   `xml:"id,attr"`
	Path    PagePath `xml:"path"`
}

type RootDeletePage struct {
	XMLName xml.Name `xml:"root.delete.page"`
	ID      string   `xml:"id,attr"`
	Path    PagePath `xml:"path"`
}

type File struct {
	XMLName  xml.Name `xml:"file"`
	ID       string   `xml:"id,attr"`
	ResID    string   `xml:"res-id,attr"`
	Filename string   `xml:"filename"`
}

type Data struct {
	XMLName    xml.Name `xml:"data"`
	URIHost    string   `xml:"_uri.host"`
	URIScheme  string   `xml:"_uri.scheme"`
	URIQuery   string   `xml:"_uri.query"`
	Query      string   `xml:"query"`
	Constraint string   `xml:"constraint"`
}

type Diff struct {
	XMLName    xml.Name `xml:"diff"`
	Added      string   `xml:"added"`
	Removed    string   `xml:"removed"`
	Attributes string   `xml:"attributes"`
	Structural string   `xml:"structural"`
}

type CommentContent struct {
	XMLName xml.Name `xml:"content"`
	Type    string   `xml:"type,attr"`
	Value   string   `xml:",innerxml"`
}

type Comment struct {
	XMLName xml.Name       `xml:"comment"`
	ID      string         `xml:"id,attr"`
	Content CommentContent `xml:"content"`
}

type Tag struct {
	XMLName xml.Name `xml:"tag"`
	Name    string   `xml:"name"`
	Type    string   `xml:"type"`
}

type Property struct {
	XMLName xml.Name `xml:"property"`
	ID      string   `xml:"id"`
	Name    string   `xml:"name"`
}

type Role struct {
	XMLName xml.Name `xml:"role"`
	ID      string   `xml:"id,attr"`
}

type Grant struct {
	XMLName xml.Name `xml:"grant"`
	ID      string   `xml:"id"`
	Type    string   `xml:"type"`
	Role    Role     `xml:"role"`
	User    User     `xml:"user"`
}

type WorkflowData struct {
	XMLName            xml.Name `xml:"data"`
	UserID             string   `xml:"_userid"`
	Username           string   `xml:"_username"`
	CustomerActivityID string   `xml:"_customeractivityid"`
	RequestID          string   `xml:"_requestid"`
	Email              string   `xml:"_email"`
	Search             string   `xml:"_search"`
	Path               string   `xml:"_path"`

	// TODO: There can be custom elements to deserialize. We should use the <param name="{name}">{value}</param> for the representation instead
}

type Workflow struct {
	XMLName xml.Name     `xml:"workflow"`
	Name    string       `xml:"name,attr"`
	URINext string       `xml:"uri.next"`
	Data    WorkflowData `xml:"data"`
}
