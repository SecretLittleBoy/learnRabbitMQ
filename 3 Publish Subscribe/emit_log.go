package main
import(
	"context"
	"log"
	"os"
	"strings"
	"time"
	amqp "github.com/rabbitmq/amqp091-go"
)

func bodyFrom(args []string) string {
    var s string
    if (len(args) < 2) || os.Args[1] == "" {
        s = "hello"
    } else {
        s = strings.Join(args[1:], " ")
    }
    return s
}

func main(){
	con,err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil{
		log.Panicf("Failed to connect to RabbitMQ")
	}
	defer con.Close()

	ch,err := con.Channel()
	if err != nil{
		log.Panicf("Failed to open a channel")
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"logs",
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil{
		log.Panicf("Failed to declare an exchange")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := bodyFrom(os.Args)

	err = ch.PublishWithContext(ctx,
		"logs",
		"",
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType: "text/plain",
			Body: []byte(body),
		})
	if err != nil{
		log.Panicf("Failed to publish a message")
	}

	log.Printf(" [x] Sent %s", body)
}