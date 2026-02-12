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

}
