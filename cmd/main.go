package main

import (
	"context"
	"fmt"
	"log"

	"net"

	pb "github.com/amruthpremjith/kafka-producer-test/producer"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/grpc"
)

type GRPCProducerServer struct {
	pb.UnimplementedGRPCProducerServer
	kafkaProducer *kafka.Producer
}

func (s *GRPCProducerServer) Produce(ctx context.Context, message *pb.KafkaMessage) (*pb.ProducerAck, error) {

	deliveryChan := make(chan kafka.Event)

	s.kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &message.Topic, Partition: kafka.PartitionAny},
		Value:          message.Value,
	}, deliveryChan)

	ev := <-deliveryChan
	m := ev.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		return nil, m.TopicPartition.Error
	}

	return &pb.ProducerAck{
		Offset:    int32(m.TopicPartition.Offset),
		Partition: m.TopicPartition.Partition,
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", 8081))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)
	pb.RegisterGRPCProducerServer(grpcServer, newGRPCServer())
	log.Println("Starting GRPC server")
	grpcServer.Serve(lis)

}

func newGRPCServer() *GRPCProducerServer {

	return &GRPCProducerServer{
		kafkaProducer: pb.NewServer(),
	}

}
