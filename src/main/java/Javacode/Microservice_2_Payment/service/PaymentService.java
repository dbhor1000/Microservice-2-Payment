package Javacode.Microservice_2_Payment.service;

//import JavacodeLibs.Order;
//import JavacodeLibs.PaidOrder;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.stereotype.Service;
//
//@Service
//public class PaymentService {
//
//    private final KafkaTemplate<String, PaidOrder> kafkaTemplate;
//
//    public PaymentService(KafkaTemplate<String, PaidOrder> kafkaTemplate) {
//        this.kafkaTemplate = kafkaTemplate;
//    }
//
//    @KafkaListener(topics = "new_orders", groupId = "payment-service")
//    public void processOrder(Order order) {
//        // Process the payment
//        boolean paymentSuccessful = processPayment(order);
//
//        if (paymentSuccessful) {
//            // Create a PaidOrder instance
//            PaidOrder paidOrder = new PaidOrder(order, "paymentId", true, "paymentTimestamp");
//            // Send the PaidOrder to the "payed_orders" topic
//            kafkaTemplate.send("payed_orders", paidOrder);
//        }
//    }
//
//    private boolean processPayment(Order order) {
//        // Implement your payment processing logic here
//        // Return true if payment is successful, false otherwise
//        return true;
//    }
//}

import JavacodeLibs.Order;
import JavacodeLibs.PaidOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class PaymentService {

    private static final Logger logger = LoggerFactory.getLogger(PaymentService.class);

    private final KafkaTemplate<String, PaidOrder> kafkaTemplate;

    public PaymentService(KafkaTemplate<String, PaidOrder> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "new_orders", groupId = "payment-service")
    public void processOrder(Order order, Acknowledgment acknowledgment) {
        try {
            logger.info("IN: Processing order: {}", order);
            // Process the payment
            boolean paymentSuccessful = processPayment(order);

            if (paymentSuccessful) {
                // Create a PaidOrder instance
                PaidOrder paidOrder = new PaidOrder(order, "paymentId", true, "paymentTimestamp");
                String key = paidOrder.getOrder().getOrderId();
                // Send the PaidOrder to the "payed_orders" topic
                kafkaTemplate.executeInTransaction(operations -> {
                    operations.send("payed_orders", key, paidOrder);
                    logger.info("OUT: Processed order: {}", paidOrder.getOrder().getOrderId());
                    return null;
                });
            }
        } catch (Exception e) {
            logger.error("Error processing order: {}", order, e);
        } finally {
            acknowledgment.acknowledge(); // Manual acknowledgment
        }
    }

    private boolean processPayment(Order order) {
        // Implement your payment processing logic here
        // Return true if payment is successful, false otherwise
        return true;
    }
}
