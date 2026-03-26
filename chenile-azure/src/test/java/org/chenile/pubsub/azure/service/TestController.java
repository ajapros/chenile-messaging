package org.chenile.pubsub.azure.service;

import jakarta.servlet.http.HttpServletRequest;
import org.chenile.base.response.GenericResponse;
import org.chenile.http.annotation.ChenileController;
import org.chenile.http.annotation.EventsSubscribedTo;
import org.chenile.http.handler.ControllerSupport;
import org.chenile.pubsub.model.ChenilePubSub;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;


@RestController
@ChenilePubSub
@ChenileController(value = "testService", serviceName = "testService")
public class TestController extends ControllerSupport{
	@PostMapping("/f/{num3}")
	ResponseEntity<GenericResponse<Map<String, Object>>> f(HttpServletRequest request,
														   @PathVariable("num3") int num3,
														  @RequestBody Payload payload){
		return process("f",request,num3,payload);
	}


	@PostMapping("/f1")
	@EventsSubscribedTo({"chenile"})
	ResponseEntity<GenericResponse<Map<String, Object>>> f1(HttpServletRequest request,
														   @RequestBody Payload payload){
		return process("f1",request,payload);
	}

	@PostMapping("/order")
	@EventsSubscribedTo({"order-created", "order-updated"})
	ResponseEntity<GenericResponse<Map<String, Object>>> orderEvent(HttpServletRequest request,
																 @RequestBody Payload payload){
		return process("orderEvent",request,payload);
	}

	@PostMapping("/invoice-paid")
	@EventsSubscribedTo({"invoice-paid"})
	ResponseEntity<GenericResponse<Map<String, Object>>> invoicePaid(HttpServletRequest request,
																  @RequestBody Payload payload){
		return process("invoicePaid",request,payload);
	}

	@PostMapping("/audit")
	@EventsSubscribedTo({"audit-created"})
	ResponseEntity<GenericResponse<Map<String, Object>>> auditEvent(HttpServletRequest request,
																 @RequestBody Payload payload){
		return process("auditEvent",request,payload);
	}

	@PostMapping("/dl")
	@EventsSubscribedTo({"eh2"})
	ResponseEntity<GenericResponse<Map<String, Object>>> dlHandler(HttpServletRequest request,
															@RequestBody Payload payload){
		return process("dlHandler",request,payload);
	}

}
