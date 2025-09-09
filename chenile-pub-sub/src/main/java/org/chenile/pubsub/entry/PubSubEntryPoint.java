package org.chenile.pubsub.entry;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.chenile.base.exception.ServerException;
import org.chenile.core.context.ChenileExchange;
import org.chenile.core.context.HeaderUtils;
import org.chenile.core.entrypoint.ChenileEntryPoint;
import org.chenile.core.model.ChenileConfiguration;
import org.chenile.core.model.ChenileServiceDefinition;
import org.chenile.core.model.OperationDefinition;
import org.chenile.pubsub.constants.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.List;
import java.util.Map;

import static org.chenile.pubsub.errorcodes.ErrorCodes.*;


public class PubSubEntryPoint {
	private static final Logger logger = LoggerFactory.getLogger(PubSubEntryPoint.class);
	@Autowired
	private ChenileConfiguration chenileConfiguration;
	@Autowired @Qualifier("pubSubConfig")
	Map<String,String> pubSubConfig;

	@Autowired
	private ChenileEntryPoint chenileEntryPoint;

	private final ObjectMapper objectMapper = new ObjectMapper();

	/**
	 * The entry point for MQ-TT. It puts the message into the system and extracts the response which is logged.
	 * We support asynchronous messages at this point in time.
	 * @param topic the topic where the message was received
	 * @throws Exception if there is a problem in processing the message
	 */
	 public void process(String topic, String messageContent,Map<String,Object> headers) throws Exception{
		 ChenileExchange exchange = makeExchange(topic);
		 exchange.setHeader(HeaderUtils.ENTRY_POINT, Constants.PUB_SUB_ENTRY_POINT);
		 exchange.setBody(messageContent);
		 populateHeaders(headers,exchange);
		 chenileEntryPoint.execute(exchange);
		 Object response = exchange.getResponse();
		 logger.info("Received message " + messageContent + " and handled it. Response = "
					 + objectMapper.writeValueAsString(response));
		 System.out.println("Received message " + messageContent + " and handled it. Response = "
				 + objectMapper.writeValueAsString(response));
	}

	private void populateHeaders(Map<String,Object> headers, ChenileExchange exchange){
		if (headers != null ) {
			for (Map.Entry<String,Object> prop : headers.entrySet()) {
				exchange.setHeader(prop.getKey(),String.valueOf(prop.getValue()));
			}
		}
	}

	/**
	 * topic will be in the format /some/stuff/serviceName/operationName
	 * extract the service name and operation name from the topic
	 * @param topic the topic that received this message. Used to compute service and op name
	 * @return the exchange
	 */
	private ChenileExchange makeExchange(String topic) {
		ChenileExchange exchange = new ChenileExchange();

		// Find last delimiter (either "_" or "/")
		int index = Math.max(topic.lastIndexOf("_"), topic.lastIndexOf("/"));
		if (index == -1) {
			throw new ServerException(UNSUPPORTED_TOPIC_FORMAT_FOR_OPERATION.getSubError(),
					new Object[]{topic});
		}

		String opName = topic.substring(index + 1);
		String t = topic.substring(0, index);

		int serviceIndex = Math.max(t.lastIndexOf("_"), t.lastIndexOf("/"));
		if (serviceIndex == -1) {
			throw new ServerException(UNSUPPORTED_TOPIC_FORMAT_FOR_SERVICE.getSubError(),
					new Object[]{topic});
		}

		String serviceId = t.substring(serviceIndex + 1);

		ChenileServiceDefinition serviceDefinition = chenileConfiguration.getServices().get(serviceId);
		if (serviceDefinition == null) {
			throw new ServerException(MISSING_SERVICE.getSubError(), new Object[]{topic, serviceId});
		}

		exchange.setServiceDefinition(serviceDefinition);

		List<OperationDefinition> operations = serviceDefinition.getOperations();
		for (OperationDefinition od : operations) {
			if (od.getName().equals(opName)) {
				exchange.setOperationDefinition(od);
				return exchange;
			}
		}

		throw new ServerException(MISSING_SERVICE_OPERATION.getSubError(),
				new Object[]{topic, serviceId, opName});

	}
}