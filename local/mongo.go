package local

import (
	"context"
	"io"
)

type MongoFunc func(ctx context.Context, name string,reader io.Reader)

var mongoFunc MongoFunc
