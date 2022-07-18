package producer

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/spf13/viper"
)

type configs struct {
	bootstrap         string
	security_protocol string
	sasl_mechanism    string
	sasl_username     string
	sasl_password     string
	ssl_ca_location   string
}

func readConfig() configs {
	viper.SetConfigName("application")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("")
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		log.Fatal("fatal error config file: ", err)
	}
	confs := configs{
		bootstrap:         viper.GetString("bootstrap.servers"),
		security_protocol: viper.GetString("security.protocol"),
		sasl_mechanism:    viper.GetString("sasl.mechanism"),
		sasl_username:     viper.GetString("sasl.username"),
		sasl_password:     viper.GetString("sasl.password"),
		ssl_ca_location:   viper.GetString("ssl.ca.location"),
	}
	return confs
}

func NewServer() *kafka.Producer {
	confs := readConfig()

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": confs.bootstrap,
		"security.protocol": confs.security_protocol,
		"sasl.mechanism":    confs.sasl_mechanism,
		"sasl.username":     confs.sasl_username,
		"sasl.password":     confs.sasl_password,
		"ssl.ca.location":   confs.ssl_ca_location,
		//"queue.buffering.max.messages": 1000000,
	})

	if err != nil {
		log.Println("Error creating producer: ", err)
	}

	log.Println("Kafka producer created")

	return p
}
