package server

import (
	"context"

	api "github.com/flomedja/proglog/api/v1"
)

type Config struct {
	CommitLog CommitLog
}

var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

// Produce appends a single record to the log and returns the assigned offset.
// This is the primary method for adding new records to the distributed log.
// Each record is assigned a unique, monotonically increasing offset that can
// be used later for retrieval via Consume.
//
// Parameters:
// - ctx: Request context for cancellation and timeout handling
// - req: ProduceRequest containing the record to append to the log
//
// Returns:
// - ProduceResponse with the assigned offset, or nil if an error occurred
// - error: Any error encountered during log append operation
func (s *grpcServer) Produce(_ context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	// Append the record to the commit log and get the assigned offset
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}

	// Return the assigned offset to the client
	return &api.ProduceResponse{Offset: offset}, nil
}

// Consume retrieves a single record from the log at the specified offset.
// This method provides random access to any record in the log by its unique offset.
// The offset must be valid (within the range of existing records) or an error will be returned.
//
// Parameters:
// - ctx: Request context for cancellation and timeout handling
// - req: ConsumeRequest containing the offset of the record to retrieve
//
// Returns:
// - ConsumeResponse with the requested record, or nil if an error occurred
// - error: Any error encountered during read operation, including ErrOffsetOutOfRange
func (s *grpcServer) Consume(_ context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	// Read the record from the commit log at the specified offset
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}

	// Return the retrieved record to the client
	return &api.ConsumeResponse{Record: record}, nil
}

// ProduceStream implements bidirectional streaming for producing multiple records to the log.
// This function continuously receives ProduceRequest messages from the client and processes
// each one by appending the record to the log, then sends back a ProduceResponse with the
// assigned offset. This enables high-throughput batch operations and real-time log ingestion.
//
// Parameters:
// - stream: Bidirectional streaming interface for receiving ProduceRequest and sending ProduceResponse
//
// The function runs indefinitely until:
// - The client closes the stream (Recv returns io.EOF)
// - A network error occurs during Recv or Send operations
// - An error occurs while appending to the log
//
// Returns any error encountered during streaming operations.
func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		// Receive the next produce request from the client stream
		req, err := stream.Recv()
		if err != nil {
			// This includes io.EOF when client closes the stream
			return err
		}

		// Process the request using the standard Produce method
		// This appends the record to the log and returns the assigned offset
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			// Return any error from log append operation
			return err
		}

		// Send the response back to the client with the assigned offset
		if err := stream.Send(res); err != nil {
			// Return any error from network send operation
			return err
		}
	}
}

// ConsumeStream implements streaming consumption of log records starting from a given offset.
// This function continuously reads records from the log and streams them to the client,
// automatically advancing the offset for each subsequent record. It handles cases where
// the requested offset is beyond the current log size by waiting and retrying.
//
// Parameters:
// - req: ConsumeRequest containing the starting offset to begin streaming from
// - stream: Server-side streaming interface for sending ConsumeResponse messages to client
//
// The function runs indefinitely until:
// - The client cancels the stream (context is done)
// - An unexpected error occurs during reading or sending
//
// Returns any error encountered during streaming operations.
func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		// Check if client has cancelled the stream or context has timed out
		case <-stream.Context().Done():
			return nil
		default:
			// Attempt to consume the record at the current offset
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
				// Successfully read a record, continue to send it
			case api.ErrOffsetOutOfRange:
				// Offset is beyond current log size, continue polling until more records arrive
				// This allows the stream to wait for new records to be produced
				continue
			default:
				// Any other error (e.g., I/O error, corruption) should terminate the stream
				return err
			}

			// Send the successfully read record to the client
			if err = stream.Send(res); err != nil {
				return err
			}

			// Move to the next offset for the subsequent iteration
			// This creates a continuous stream of records in order
			req.Offset++
		}
	}
}
