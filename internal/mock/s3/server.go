/*
Copyright 2022 The Flux authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package s3

import (
	"crypto/md5"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Object is a mock Server object.
type Object struct {
	Key          string
	LastModified time.Time
	ContentType  string
	Content      []byte
	UserMetadata map[string]string
}

type multipartUpload struct {
	key          string
	contentType  string
	userMetadata map[string]string
	parts        map[int][]byte
}

// Server is a simple AWS S3 mock server.
// It serves the provided Objects for the BucketName on the HTTPAddress when
// Start or StartTLS is called.
type Server struct {
	srv *httptest.Server
	mux *http.ServeMux

	BucketName string
	Objects    []*Object
	// AccessKey is the access key requests must be signed with.
	// If empty, requests are not checked for credentials.
	AccessKey string

	FailNext  bool
	DelayNext time.Duration

	mu               sync.Mutex
	multipartUploads map[string]*multipartUpload
}

func NewServer(bucketName string) *Server {
	s := &Server{
		BucketName:       bucketName,
		multipartUploads: make(map[string]*multipartUpload),
	}
	s.mux = http.NewServeMux()
	s.mux.Handle("/", http.HandlerFunc(s.handler))

	s.srv = httptest.NewUnstartedServer(s.mux)

	return s
}

func (s *Server) Start() {
	s.srv.Start()
}

func (s *Server) StartTLS(config *tls.Config) {
	s.srv.TLS = config
	s.srv.StartTLS()
}

func (s *Server) Stop() {
	s.srv.Close()
}

func (s *Server) HTTPAddress() string {
	return s.srv.URL
}

func (s *Server) Object(key string) *Object {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, o := range s.Objects {
		if o.Key == key {
			cp := *o
			cp.Content = append([]byte(nil), o.Content...)
			cp.UserMetadata = cloneMetadata(o.UserMetadata)
			return &cp
		}
	}
	return nil
}

func (s *Server) ObjectCount(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	for _, o := range s.Objects {
		if o.Key == key {
			count++
		}
	}
	return count
}

func (s *Server) DeleteObject(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	objects := s.Objects[:0]
	for _, o := range s.Objects {
		if o.Key != key {
			objects = append(objects, o)
		}
	}
	s.Objects = objects
}

func (s *Server) handler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	delay := s.DelayNext
	s.DelayNext = 0
	fail := s.FailNext
	s.FailNext = false
	s.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
	}
	if fail {
		writeError(w, r, http.StatusServiceUnavailable, "ServiceUnavailable", "Simulated failure.")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.AccessKey != "" && !strings.Contains(r.Header.Get("Authorization"), "Credential="+s.AccessKey+"/") {
		writeError(w, r, http.StatusForbidden, "AccessDenied", "Access Denied.")
		return
	}

	if r.URL.Path == "/"+s.BucketName || r.URL.Path == "/"+s.BucketName+"/" {
		s.handleBucket(w, r)
		return
	}

	prefix := "/" + s.BucketName + "/"
	if !strings.HasPrefix(r.URL.Path, prefix) {
		writeError(w, r, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
		return
	}
	key := strings.TrimPrefix(r.URL.Path, prefix)

	if r.Method == http.MethodPost && r.URL.Query().Has("uploads") {
		s.beginMultipart(w, r, key)
		return
	}
	if r.Method == http.MethodPut && r.URL.Query().Has("partNumber") && r.URL.Query().Has("uploadId") {
		s.putMultipartPart(w, r)
		return
	}
	if r.Method == http.MethodPost && r.URL.Query().Has("uploadId") {
		s.completeMultipart(w, r)
		return
	}
	if r.Method == http.MethodDelete && r.URL.Query().Has("uploadId") {
		delete(s.multipartUploads, r.URL.Query().Get("uploadId"))
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method == http.MethodPut {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "InvalidRequest", err.Error())
			return
		}
		s.upsertObject(&Object{
			Key:          key,
			LastModified: time.Now(),
			ContentType:  r.Header.Get("Content-Type"),
			Content:      data,
			UserMetadata: extractMeta(r.Header),
		})
		etag := md5.Sum(data)
		w.Header().Set("ETag", fmt.Sprintf("\"%x\"", etag))
		w.WriteHeader(http.StatusOK)
		return
	}

	found := s.findObject(key)
	if found == nil {
		writeError(w, r, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
		return
	}

	etag := md5.Sum(found.Content)
	lastModified := strings.Replace(found.LastModified.UTC().Format(time.RFC1123), "UTC", "GMT", 1)

	w.Header().Add("Content-Type", found.ContentType)
	w.Header().Add("Last-Modified", lastModified)
	w.Header().Add("ETag", fmt.Sprintf("\"%x\"", etag))
	w.Header().Add("Content-Length", fmt.Sprintf("%d", len(found.Content)))
	for k, v := range found.UserMetadata {
		w.Header().Add("x-amz-meta-"+k, v)
	}

	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(found.Content)
}

func (s *Server) handleBucket(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/xml")

	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.URL.Query().Has("location") {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`
<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">Europe</LocationConstraint>
		`))
		return
	}
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	contents := ""
	for _, o := range s.Objects {
		etag := md5.Sum(o.Content)
		contents += fmt.Sprintf(`
	<Contents>
		<Key>%s</Key>
		<LastModified>%s</LastModified>
		<Size>%d</Size>
		<ETag>&quot;%x&quot;</ETag>
		<StorageClass>STANDARD</StorageClass>
	</Contents>`, o.Key, o.LastModified.UTC().Format(time.RFC3339), len(o.Content), etag)
	}

	fmt.Fprintf(w, `
<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<Name>%s</Name>
	<Prefix/>
	<Marker/>
	<KeyCount>%d</KeyCount>
	<MaxKeys>1000</MaxKeys>
	<IsTruncated>false</IsTruncated>
	%s
</ListBucketResult>
	`, s.BucketName, len(s.Objects), contents)
}

func (s *Server) beginMultipart(w http.ResponseWriter, r *http.Request, key string) {
	uploadID := fmt.Sprintf("upload-%d", time.Now().UnixNano())
	s.multipartUploads[uploadID] = &multipartUpload{
		key:          key,
		contentType:  r.Header.Get("Content-Type"),
		userMetadata: extractMeta(r.Header),
		parts:        make(map[int][]byte),
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<Bucket>%s</Bucket>
	<Key>%s</Key>
	<UploadId>%s</UploadId>
</InitiateMultipartUploadResult>`, s.BucketName, key, uploadID)
}

func (s *Server) putMultipartPart(w http.ResponseWriter, r *http.Request) {
	uploadID := r.URL.Query().Get("uploadId")
	upload, ok := s.multipartUploads[uploadID]
	if !ok {
		writeError(w, r, http.StatusNotFound, "NoSuchUpload", "The specified upload does not exist.")
		return
	}

	partNumber, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if err != nil || partNumber <= 0 {
		writeError(w, r, http.StatusBadRequest, "InvalidArgument", "Invalid part number.")
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}

	upload.parts[partNumber] = data
	etag := md5.Sum(data)
	w.Header().Set("ETag", fmt.Sprintf("\"%x\"", etag))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) completeMultipart(w http.ResponseWriter, r *http.Request) {
	uploadID := r.URL.Query().Get("uploadId")
	upload, ok := s.multipartUploads[uploadID]
	if !ok {
		writeError(w, r, http.StatusNotFound, "NoSuchUpload", "The specified upload does not exist.")
		return
	}

	partNumbers := make([]int, 0, len(upload.parts))
	for partNumber := range upload.parts {
		partNumbers = append(partNumbers, partNumber)
	}
	sort.Ints(partNumbers)

	var assembled []byte
	for _, partNumber := range partNumbers {
		assembled = append(assembled, upload.parts[partNumber]...)
	}

	s.upsertObject(&Object{
		Key:          upload.key,
		LastModified: time.Now(),
		ContentType:  upload.contentType,
		Content:      assembled,
		UserMetadata: upload.userMetadata,
	})
	delete(s.multipartUploads, uploadID)

	etag := md5.Sum(assembled)
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<Location>http://%s/%s/%s</Location>
	<Bucket>%s</Bucket>
	<Key>%s</Key>
	<ETag>&quot;%x&quot;</ETag>
</CompleteMultipartUploadResult>`, r.Host, s.BucketName, upload.key, s.BucketName, upload.key, etag)
}

func (s *Server) findObject(key string) *Object {
	for _, o := range s.Objects {
		if o.Key == key {
			return o
		}
	}
	return nil
}

func (s *Server) upsertObject(object *Object) {
	for i, o := range s.Objects {
		if o.Key == object.Key {
			s.Objects[i] = object
			return
		}
	}
	s.Objects = append(s.Objects, object)
}

func extractMeta(h http.Header) map[string]string {
	m := make(map[string]string)
	for k, values := range h {
		lower := strings.ToLower(k)
		if strings.HasPrefix(lower, "x-amz-meta-") && len(values) > 0 {
			m[strings.TrimPrefix(lower, "x-amz-meta-")] = values[0]
		}
	}
	return m
}

func cloneMetadata(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// writeError writes an S3 error response, the body is omitted for HEAD requests.
func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	w.Header().Add("Content-Type", "application/xml")
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>%s</Code><Message>%s</Message></Error>`, code, message)
	}
}
