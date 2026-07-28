package core

import "google.golang.org/protobuf/proto"

type Row interface {
	proto.Message
	GetTenantId() uint32
}
