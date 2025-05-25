package ma.pupu.sqs.event.handler;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.MessageAttribute;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import com.fasterxml.jackson.databind.ObjectMapper;

import ma.pupu.sqs.event.dto.Member;

public class SQSEventHandler implements RequestHandler<SQSEvent, Boolean> {

	@Override
	public Boolean handleRequest(SQSEvent sqsEvent, Context context) {
		LambdaLogger logger = context.getLogger();
		logger.log("SQSEventHandler.handleRequest:: START");

		for (SQSMessage message : sqsEvent.getRecords()) {
			
			// message body
			String messageBody = message.getBody();
			logger.log("SQSEventHandler.handleRequest :: messageBody= " + messageBody, LogLevel.INFO);
			
			// convert the JSON to object
			List<Member> memberList = getMemberList(messageBody, logger);
			if(memberList != null && memberList.size() > 0) {
				for(Member member : memberList) {
					logger.log("SQSEventHandler.handleRequest :: FirstName=" + member.getFirstName() + "; LastName=" + member.getLastName(), LogLevel.INFO);
				}
			}

			// message attribute if any
			Map<String, MessageAttribute> msgAttributes = message.getMessageAttributes();
			if (msgAttributes != null && msgAttributes.size() > 0) {
				Set<String> attributeSet = msgAttributes.keySet();

				for (String key : attributeSet) {
					MessageAttribute attribute = msgAttributes.get(key);
					logger.log("SQSEventHandler.handleRequest :: datatype=" + attribute.getDataType() + "; StringValue=" + attribute.getStringValue(), LogLevel.INFO);
				}
			}
		}
		logger.log("SQSEventHandler.handleRequest:: END");
		return true;
	}
	
	
	private List<Member> getMemberList(String jsonString, LambdaLogger logger) {
		ObjectMapper objectMapper = new ObjectMapper();
		List<Member> memberList = null;
        try {
            memberList = objectMapper.readValue(jsonString, List.class);
        } catch (Exception e) {
        	logger.log("SQSEventHandler.getMemberList:: Error: " + e.getMessage(), LogLevel.ERROR);
        }
        
        return memberList;
	}
}
