package org.chenile.pubsub.azure.service;

import jakarta.servlet.http.HttpServletRequest;
import org.chenile.base.response.GenericResponse;
import org.chenile.http.annotation.ChenileController;
import org.chenile.http.annotation.EventsSubscribedTo;
import org.chenile.http.handler.ControllerSupport;
import org.chenile.pubsub.model.ChenilePubSub;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@ChenilePubSub
@ChenileController(value = "routingTestService", serviceName = "routingTestService")
public class RoutingTestController extends ControllerSupport {
	@PostMapping("/shipment")
	@EventsSubscribedTo({"shipment-created", "shipment-updated"})
	ResponseEntity<GenericResponse<Map<String, Object>>> shipmentEvent(HttpServletRequest request,
																	 @RequestBody Payload payload) {
		return process("shipmentEvent", request, payload);
	}

	@PostMapping("/refund")
	@EventsSubscribedTo({"refund-issued"})
	ResponseEntity<GenericResponse<Map<String, Object>>> refundEvent(HttpServletRequest request,
																   @RequestBody Payload payload) {
		return process("refundEvent", request, payload);
	}

	@PostMapping("/customer-alert")
	@EventsSubscribedTo({"customer-alert", "customer-followup"})
	ResponseEntity<GenericResponse<Map<String, Object>>> customerAlert(HttpServletRequest request,
																	 @RequestBody Payload payload) {
		return process("customerAlert", request, payload);
	}
}
