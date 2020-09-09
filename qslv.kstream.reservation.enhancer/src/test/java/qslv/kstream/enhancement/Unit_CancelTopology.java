package qslv.kstream.enhancement;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.util.UUID;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import qslv.common.kafka.TraceableMessage;
import qslv.data.Account;
import qslv.kstream.CancelReservationRequest;
import qslv.kstream.LoggedTransaction;
import qslv.kstream.workflow.CancelReservationWorkflow;
import qslv.kstream.workflow.WorkflowMessage;
import qslv.util.Random;

@ExtendWith(MockitoExtension.class)
class Unit_CancelTopology {

	public final static String AIT = "237482"; 
	public final static String TEST_TAXONOMY_ID = "9.9.9.9.9";
	public final static String CORRELATION_ID = UUID.randomUUID().toString();
	public final static String VALID_STATUS = "EF";
	public final static String INVALID_STATUS = "CL";
	public final static String JSON_DATA = "{\"value\": 234934}";

	static TestSetup context;
	
	@BeforeAll
	static void beforeAl() throws Exception {
		context = new TestSetup();
	}
	
	@AfterAll
	static void afterAll() {
		context.getTestDriver().close();
	}
	
	private void drain(TestOutputTopic<?, ?> topic) {
		while ( topic.getQueueSize() > 0) {
			topic.readKeyValue();
		}
	}

	@Test
	void test_cancel_success() {
    	drain(context.getEnhancedRequestTopic());

    	// Setup 
    	CancelReservationRequest request = setupCancelRequest();
    	TraceableMessage<WorkflowMessage> traceable = setupTraceableMessage(setupWorkflow(request));
    	LoggedTransaction reservation = setupReservation(request.getAccountNumber(), request.getReservationUuid());
    	
    	// Execute
    	context.getTransactionByUuidTopic().pipeInput(reservation.getTransactionUuid(), reservation);
    	context.getMatchReservationTopic().pipeInput(request.getReservationUuid(), traceable);
    	KeyValue<String, TraceableMessage<WorkflowMessage>> keyvalue =
    			context.getEnhancedRequestTopic().readKeyValue();
    	
    	// -- verify -----
    	assertEquals(keyvalue.key, request.getAccountNumber());
    	assertNotNull(keyvalue.value);
    	verifyTraceData(traceable, keyvalue.value);
    	assertNotNull(keyvalue.value.getPayload());
    	
    	CancelReservationWorkflow crw = keyvalue.value.getPayload().getCancelReservationWorkflow();
    	assertNull( crw.getAccumulatedResults() );
		assertNull( crw.getErrorMessage() );
    	assertNull( crw.getResults() );
    	assertEquals( crw.getProcessingAccountNumber(), request.getAccountNumber() );
    	assertEquals( crw.getState(), CancelReservationWorkflow.CANCEL_RESERVATION );
    	verifyCancelRequest( crw.getRequest(), request);
    	verifyAccount( crw.getAccount(), traceable.getPayload().getCancelReservationWorkflow().getAccount());
    	verifyTransactions( crw.getReservation(), reservation);
	}
	
	@Test
	void test_cancel_no_match() {
    	drain(context.getEnhancedRequestTopic());

    	// Setup 
    	CancelReservationRequest request = setupCancelRequest();
    	TraceableMessage<WorkflowMessage> traceable = setupTraceableMessage(setupWorkflow(request));
    	LoggedTransaction reservation = setupReservation(request.getAccountNumber(), UUID.randomUUID());
    	
    	// Execute
    	context.getTransactionByUuidTopic().pipeInput(reservation.getTransactionUuid(), reservation);
    	context.getMatchReservationTopic().pipeInput(request.getReservationUuid(), traceable);
    	KeyValue<String, TraceableMessage<WorkflowMessage>> keyvalue =
    			context.getEnhancedRequestTopic().readKeyValue();
    	
    	// -- verify -----
    	assertEquals(keyvalue.key, request.getAccountNumber());
    	assertNotNull(keyvalue.value);
    	verifyTraceData(traceable, keyvalue.value);
    	assertNotNull(keyvalue.value.getPayload());
    	
    	CancelReservationWorkflow crw = keyvalue.value.getPayload().getCancelReservationWorkflow();
    	assertNull( crw.getAccumulatedResults() );
		assertNull( crw.getErrorMessage() );
    	assertNull( crw.getResults() );
    	assertNull( crw.getReservation() );
    	assertEquals( crw.getProcessingAccountNumber(), request.getAccountNumber() );
    	assertEquals( crw.getState(), CancelReservationWorkflow.NO_MATCH );
    	verifyCancelRequest( crw.getRequest(), request);
    	verifyAccount( crw.getAccount(), traceable.getPayload().getCancelReservationWorkflow().getAccount());
	}
	
	@Test
	void test_cancel_already_committed() {
    	drain(context.getEnhancedRequestTopic());

    	// Setup 
    	CancelReservationRequest request = setupCancelRequest();
    	TraceableMessage<WorkflowMessage> traceable = setupTraceableMessage(setupWorkflow(request));
    	LoggedTransaction reservation = setupReservation(request.getAccountNumber(), request.getReservationUuid());
    	LoggedTransaction commit = setupCommitOrCancel(reservation, LoggedTransaction.RESERVATION_COMMIT);
    	
    	// Execute
    	context.getTransactionByUuidTopic().pipeInput(reservation.getTransactionUuid(), reservation);
    	context.getTransactionByUuidTopic().pipeInput(commit.getReservationUuid(), commit);
    	context.getMatchReservationTopic().pipeInput(request.getReservationUuid(), traceable);
    	KeyValue<String, TraceableMessage<WorkflowMessage>> keyvalue =
    			context.getEnhancedRequestTopic().readKeyValue();
    	
    	// -- verify -----
    	assertEquals(keyvalue.key, request.getAccountNumber());
    	assertNotNull(keyvalue.value);
    	verifyTraceData(traceable, keyvalue.value);
    	assertNotNull(keyvalue.value.getPayload());
    	
    	CancelReservationWorkflow crw = keyvalue.value.getPayload().getCancelReservationWorkflow();
    	assertNull( crw.getAccumulatedResults() );
		assertNull( crw.getErrorMessage() );
    	assertNull( crw.getResults() );
    	assertNull( crw.getReservation() );
    	assertEquals( crw.getProcessingAccountNumber(), request.getAccountNumber() );
    	assertEquals( crw.getState(), CancelReservationWorkflow.NO_MATCH );
    	verifyCancelRequest( crw.getRequest(), request);
    	verifyAccount( crw.getAccount(), traceable.getPayload().getCancelReservationWorkflow().getAccount());
	}
	
	@Test
	void test_cancel_already_cancelled() {
    	drain(context.getEnhancedRequestTopic());

    	// Setup 
    	CancelReservationRequest request = setupCancelRequest();
    	TraceableMessage<WorkflowMessage> traceable = setupTraceableMessage(setupWorkflow(request));
    	LoggedTransaction reservation = setupReservation(request.getAccountNumber(), request.getReservationUuid());
    	LoggedTransaction cancel = setupCommitOrCancel(reservation, LoggedTransaction.RESERVATION_CANCEL);
    	
    	// Execute
    	context.getTransactionByUuidTopic().pipeInput(reservation.getTransactionUuid(), reservation);
    	context.getTransactionByUuidTopic().pipeInput(cancel.getReservationUuid(), cancel);
    	context.getMatchReservationTopic().pipeInput(request.getReservationUuid(), traceable);
    	KeyValue<String, TraceableMessage<WorkflowMessage>> keyvalue =
    			context.getEnhancedRequestTopic().readKeyValue();
    	
    	// -- verify -----
    	assertEquals(keyvalue.key, request.getAccountNumber());
    	assertNotNull(keyvalue.value);
    	verifyTraceData(traceable, keyvalue.value);
    	assertNotNull(keyvalue.value.getPayload());
    	
    	CancelReservationWorkflow crw = keyvalue.value.getPayload().getCancelReservationWorkflow();
    	assertNull( crw.getAccumulatedResults() );
		assertNull( crw.getErrorMessage() );
    	assertNull( crw.getResults() );
    	assertNull( crw.getReservation() );
    	assertEquals( crw.getProcessingAccountNumber(), request.getAccountNumber() );
    	assertEquals( crw.getState(), CancelReservationWorkflow.NO_MATCH );
    	verifyCancelRequest( crw.getRequest(), request);
    	verifyAccount( crw.getAccount(), traceable.getPayload().getCancelReservationWorkflow().getAccount());
	}

	void verifyAccount(Account expected, Account actual) {
		assertEquals(expected.getAccountLifeCycleStatus(), actual.getAccountLifeCycleStatus());		
		assertEquals(expected.getAccountNumber(), actual.getAccountNumber());
	}

	Account setupAccount(String accountNumber, boolean effective) {
		Account account = new Account();
		account.setAccountNumber(accountNumber);
		account.setAccountLifeCycleStatus(effective ? "EF" : "CL");
		return account;
	}
	
	CancelReservationRequest setupCancelRequest() {
		CancelReservationRequest request = new CancelReservationRequest();
		request.setAccountNumber(Random.randomDigits(12));
		request.setJsonMetaData(JSON_DATA);
		request.setRequestUuid(UUID.randomUUID());
		request.setReservationUuid(UUID.randomUUID());
		return request;
	}
	
	WorkflowMessage setupWorkflow(CancelReservationRequest request) {
		CancelReservationWorkflow workflow = new CancelReservationWorkflow(CancelReservationWorkflow.MATCH_RESERVATION, request);
		workflow.setAccount(setupAccount(request.getAccountNumber(), true));
		workflow.setProcessingAccountNumber(request.getAccountNumber());
		return new WorkflowMessage(workflow);
	}
	
	TraceableMessage<WorkflowMessage> setupTraceableMessage(WorkflowMessage payload) {
		TraceableMessage<WorkflowMessage> message = new TraceableMessage<>();
		message.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		message.setCorrelationId(CORRELATION_ID);
		message.setMessageCreationTime(LocalDateTime.now());
		message.setPayload(payload);
		message.setProducerAit(AIT);
		return message;
	}

	LoggedTransaction setupReservation(String accountNumber, UUID reservationId) {
		LoggedTransaction reservation = new LoggedTransaction();
		reservation.setAccountNumber(accountNumber);
		reservation.setDebitCardNumber(Random.randomDigits(16));
		reservation.setRequestUuid(UUID.randomUUID());
		reservation.setRunningBalanceAmount(Random.randomLong());
		reservation.setTransactionAmount(Random.randomLong());
		reservation.setTransactionMetaDataJson(JSON_DATA);
		reservation.setTransactionTime(LocalDateTime.now());
		reservation.setTransactionTypeCode(LoggedTransaction.RESERVATION);
		reservation.setTransactionUuid(reservationId);
		return reservation;
	}
	LoggedTransaction setupCommitOrCancel( LoggedTransaction reservation, String type ) {
		LoggedTransaction commit = new LoggedTransaction();
		commit.setAccountNumber(reservation.getAccountNumber());
		commit.setDebitCardNumber(reservation.getDebitCardNumber());
		commit.setRequestUuid(UUID.randomUUID());
		commit.setRunningBalanceAmount(Random.randomLong());
		commit.setTransactionAmount(0L);
		commit.setTransactionMetaDataJson(JSON_DATA);
		commit.setTransactionTime(LocalDateTime.now());
		commit.setTransactionTypeCode(type);
		commit.setTransactionUuid(UUID.randomUUID());
		commit.setReservationUuid(reservation.getTransactionUuid());
		return commit;
		
	}
	private void verifyTraceData(TraceableMessage<?> expected, TraceableMessage<?> actual) {
		assertEquals(expected.getBusinessTaxonomyId(), actual.getBusinessTaxonomyId());
		assertEquals(expected.getCorrelationId(), actual.getCorrelationId());
		assertEquals(expected.getMessageCreationTime(), actual.getMessageCreationTime());
		assertEquals(expected.getProducerAit(), actual.getProducerAit());
		assertEquals(expected.getMessageCompletionTime(), actual.getMessageCompletionTime());
	}
	private void verifyCancelRequest(CancelReservationRequest expected, CancelReservationRequest actual) {
		assertEquals( expected.getAccountNumber(), actual.getAccountNumber());
		assertEquals( expected.getRequestUuid(), actual.getRequestUuid());
		assertEquals( expected.getJsonMetaData(), actual.getJsonMetaData());
		assertEquals( expected.getReservationUuid(), actual.getReservationUuid());
	}
	private void verifyTransactions(LoggedTransaction expected, LoggedTransaction actual) {
		assertEquals( expected.getAccountNumber(), actual.getAccountNumber());
		assertEquals( expected.getDebitCardNumber(), actual.getDebitCardNumber());
		assertEquals( expected.getRequestUuid(), actual.getRequestUuid());
		assertEquals( expected.getReservationUuid(), actual.getReservationUuid());
		assertEquals( expected.getRunningBalanceAmount(), actual.getRunningBalanceAmount());
		assertEquals( expected.getTransactionAmount(), actual.getTransactionAmount());
		assertEquals( expected.getTransactionMetaDataJson(), actual.getTransactionMetaDataJson());
		assertEquals( expected.getTransactionTime(), actual.getTransactionTime());
		assertEquals( expected.getTransactionTypeCode(), actual.getTransactionTypeCode());
		assertEquals( expected.getTransactionUuid(), actual.getTransactionUuid());
	}

}
