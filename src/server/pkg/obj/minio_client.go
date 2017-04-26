package obj

import (
	"fmt"
	"io"
	"log"
	"os"

	minio "github.com/minio/minio-go"
)

// Represents minio client instance for any s3 compatible server.
type minioClient struct {
	*minio.Client
	bucket string

	logger *log.Logger
}

// Creates a new minioClient structure and returns
func newMinioClient(endpoint, bucket, id, secret string, secure bool) (*minioClient, error) {
	mclient, err := minio.New(endpoint, id, secret, secure)
	if err != nil {
		return nil, err
	}
	return &minioClient{
		bucket: bucket,
		Client: mclient,
		logger: log.New(os.Stdout, "[MinioClient] ", log.LstdFlags|log.Lshortfile),
	}, nil
}

// Represents minio writer structure with pipe and the error channel
type minioWriter struct {
	errChan chan error
	pipe    *io.PipeWriter

	logger *log.Logger
}

// Creates a new minio writer and a go routine to upload objects to minio server
func newMinioWriter(client *minioClient, name string) *minioWriter {
	reader, writer := io.Pipe()
	w := &minioWriter{
		errChan: make(chan error),
		pipe:    writer,
		logger:  log.New(os.Stdout, fmt.Sprintf("[MinioWriter/%s] ", name), log.LstdFlags|log.Lshortfile),
	}
	go func() {
		// TODO(msteffen) (fix double close):
		// defer close(w.errChan)
		client.logger.Printf("beginning PutObject(%s)", name)
		_, err := client.PutObject(client.bucket, name, reader, "application/octet-stream")
		client.logger.Printf("finished PutObject(%s) error: %s", name, err)
		if err != nil {
			reader.CloseWithError(err)
		}
		w.errChan <- err
	}()
	return w
}

func (w *minioWriter) Write(p []byte) (int, error) {
	w.logger.Printf("passing %d bytes to MinioWriter", len(p))
	return w.pipe.Write(p)
}

// This will block till upload is done
func (w *minioWriter) Close() error {
	w.logger.Printf("closing pipe")
	if err := w.pipe.Close(); err != nil {
		return err
	}
	err := <-w.errChan
	w.logger.Printf("Close() has finished with err: %s", err)
	return err
}

func (c *minioClient) Writer(name string) (io.WriteCloser, error) {
	return newMinioWriter(c, name), nil
}

func (c *minioClient) Walk(name string, fn func(name string) error) error {
	recursive := true // Recursively walk by default.

	doneCh := make(chan struct{})
	defer close(doneCh)
	for objInfo := range c.ListObjectsV2(c.bucket, name, recursive, doneCh) {
		if objInfo.Err != nil {
			return objInfo.Err
		}
		if err := fn(objInfo.Key); err != nil {
			return err
		}
	}
	return nil
}

// limitReadCloser implements a closer compatible wrapper
// for a size limited reader.
type limitReadCloser struct {
	reader io.Reader
	mObj   *minio.Object

	logger *log.Logger
}

func (l *limitReadCloser) Read(p []byte) (n int, err error) {
	n, err = l.reader.Read(p)
	l.logger.Printf("read %d bytes, with err: %s", n, err)
	return
}

func (l *limitReadCloser) Close() (err error) {
	l.logger.Printf("closing reader")
	return l.mObj.Close()
}

func (c *minioClient) Reader(name string, offset uint64, size uint64) (io.ReadCloser, error) {
	logger := log.New(os.Stdout, fmt.Sprintf("[MinioReader/%s] ", name), log.LstdFlags|log.Lshortfile)

	logger.Printf("reading %d bytes at offset: %d", size, offset)
	obj, err := c.GetObject(c.bucket, name)
	if err != nil {
		logger.Printf("GetObject error: %s", err)
		return nil, err
	}
	// Seek to an offset to fetch the new reader.
	_, err = obj.Seek(int64(offset), 0)
	if err != nil {
		logger.Printf("Seek error: %s", err)
		return nil, err
	}
	// TODO(msteffen) fix overflow:
	// signedSize := int64(size) > 0 ? int64(size) :  math.MaxInt64)
	if size > 0 {
		return &limitReadCloser{
			reader: io.LimitReader(obj, int64(size)),
			mObj:   obj,
			logger: logger,
		}, nil
	}
	return obj, nil

}

func (c *minioClient) Delete(name string) (err error) {
	err = c.RemoveObject(c.bucket, name)
	c.logger.Printf("Delete(%s) yielded %s", name, err)
	return
}

func (c *minioClient) Exists(name string) bool {
	_, err := c.StatObject(c.bucket, name)
	c.logger.Printf("Exists(%s) yields %s (exists == %t)", name, err, err == nil)
	// TODO(msteffen): catch bad errors:
	// return !c.IsNotExist(err)
	return err == nil
}

func (c *minioClient) isRetryable(err error) bool {
	// Minio client already implements retrying, no
	// need for a caller retry.
	return false
}

func (c *minioClient) IsIgnorable(err error) bool {
	return false
}

// Sentinel error response returned if err is not
// of type *minio.ErrorResponse.
var sentinelErrResp = minio.ErrorResponse{}

func (c *minioClient) IsNotExist(err error) bool {
	errResp := minio.ToErrorResponse(err)
	if errResp == sentinelErrResp {
		return false
	}
	c.logger.Printf("IsNotExists(%s) yields \"%+v\" (IsNotExist == %t)", err,
		errResp, errResp.Code == "NoSuchKey" || errResp.Code == "NoSuchBucket")
	// Treat both object not found and bucket not found as IsNotExist().
	return errResp.Code == "NoSuchKey" || errResp.Code == "NoSuchBucket"
}
