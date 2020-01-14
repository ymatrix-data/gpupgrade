package idl

// MessageSender is an interface common to all gRPC streaming server
// implementations that allows the sending of a Message struct.
type MessageSender interface {
	Send(*Message) error // matches gRPC streaming Send()
}
