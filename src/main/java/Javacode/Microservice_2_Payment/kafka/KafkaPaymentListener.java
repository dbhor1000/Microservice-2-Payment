package Javacode.Microservice_2_Payment.kafka;

import Javacode.Microservice_2_Payment.service.PaymentService;
import JavacodeLibs.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class KafkaPaymentListener {

    Logger logger = LoggerFactory.getLogger(KafkaPaymentListener.class);

    private final PaymentService paymentService;


    public KafkaPaymentListener(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @KafkaListener(topics = "new_orders", groupId = "payment-service")
    public void processOrder(Order order, Acknowledgment acknowledgment) {
        try {
            logger.info("IN: Processing order: {}", order);
            // Process the payment
            paymentService.processOrder(order, acknowledgment);
        } catch (Exception e) {
            logger.error("Error processing order: {}", order, e);
        }
    }
}
