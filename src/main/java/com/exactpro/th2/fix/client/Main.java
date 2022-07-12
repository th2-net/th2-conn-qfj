package com.exactpro.th2.fix.client;

import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.MessageGroupBatch;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter;
import com.exactpro.th2.common.schema.message.MessageListener;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.MessageRouterUtils;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.common.utils.event.EventBatcher;
import com.exactpro.th2.common.utils.event.MessageBatcher;
import com.exactpro.th2.fix.client.exceptions.CreatingConfigFileException;
import com.exactpro.th2.fix.client.exceptions.IncorrectFixFileNameException;
import com.exactpro.th2.fix.client.fixBean.BaseFixBean;
import com.exactpro.th2.fix.client.fixBean.FixBean;
import com.exactpro.th2.fix.client.impl.Destructor;
import com.exactpro.th2.fix.client.util.EventUtil;
import com.exactpro.th2.fix.client.util.FixBeanUtil;
import com.exactpro.th2.fix.client.util.MessageUtil;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.json.JsonMapper;
import kotlin.Unit;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.concurrent.ConcurrentException;
import org.apache.commons.lang3.concurrent.LazyInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.ConfigError;
import quickfix.DataDictionary;
import quickfix.IncorrectDataFormat;
import quickfix.MessageUtils;
import quickfix.RuntimeError;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionSettings;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.exactpro.th2.common.message.MessageUtils.toJson;
import static com.exactpro.th2.common.schema.message.QueueAttribute.EVENT;
import static com.exactpro.th2.common.schema.message.QueueAttribute.PUBLISH;

public class Main {

    private static final String INPUT_QUEUE_ATTRIBUTE = "send";
    private static final String YES_SETTING = "Y";
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private static final int THREADS_FOR_MESSAGES_AND_EVENTS_BATCHER = 2;

    public static class Resources {
        private final String name;
        private final Destructor destructor;

        public String getName() {
            return name;
        }

        public Destructor getDestructor() {
            return destructor;
        }

        public Resources(String name, Destructor destructor) {
            this.name = name;
            this.destructor = destructor;
        }
    }

    public static void main(String[] args) throws Exception {
        ConcurrentLinkedDeque<Resources> resources = new ConcurrentLinkedDeque<>();
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(
                    () -> resources.descendingIterator().forEachRemaining(resource -> {
                        LOGGER.debug("Destroying resource: " + resource.name);
                        try {
                            resource.destructor.close();
                        } catch (Exception e) {
                            LOGGER.error("Failed to destroy resource: {}", resource.name);
                        }
                    })
            ));
        } catch (RuntimeException e) {
            LOGGER.error("Uncaught exception. Shutting down");
            System.exit(1);
            throw new RuntimeException("System.exit returned normally, while it was supposed to halt JVM.", e);
        }

        CommonFactory factory;
        try {
            factory = CommonFactory.createFromArguments(args);
            resources.add(new Resources("factory", factory::close));
        } catch (Exception e) {
            factory = new CommonFactory();
            LOGGER.error("Failed to create common factory from args", e);
        }

        JsonMapper mapper = JsonMapper.builder().build();
        Settings settings = factory.getCustomConfiguration(Settings.class, mapper);
        Path temporaryDirectory = Files.createTempDirectory("conn-qfj-dictionaries");

        if (settings.isZipDictionaries()) {
            try (InputStream rawBase64 = factory.readDictionary();
                 ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(Base64
                         .getDecoder()
                         .decode(rawBase64.readAllBytes()));
                 ZipInputStream zipInputStream = new ZipInputStream(byteArrayInputStream)) {
                ZipEntry zipEntry = null;
                try {
                    while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                        Path filePath = Path.of(zipEntry.getName());
                        if (!filePath.toString().endsWith(".xml")) {
                            throw new IncorrectFixFileNameException("Incorrect FIX dictionary file name: " + filePath.getFileName());
                        }
                        writeDictionary(temporaryDirectory, zipInputStream, filePath);
                    }
                } catch (IncorrectFixFileNameException e) {
                    throw new Exception("Failed to unzip dictionaries along the path: " + zipEntry.getName(), e);
                }
            } catch (Exception e) {
                throw new Exception("Failed to create DataDictionary", e);
            }
        } else {
            for (String dictionaryAlias : factory.getDictionaryAliases()) {
                try {
                    InputStream dictionary = factory.loadDictionary(dictionaryAlias);
                    writeDictionary(temporaryDirectory, dictionary, Path.of(dictionaryAlias));
                } catch (Exception e) {
                    throw new Exception("Failed to create DataDictionary by alias: " + dictionaryAlias, e);
                }
            }
        }

        for (FixBean sessionSetting : settings.sessionSettings) {
            SessionID sessionID = FixBeanUtil.getSessionID(sessionSetting);
            String beginString = Objects.requireNonNullElse(sessionSetting.getBeginString(), settings.getBeginString());
            if (beginString.equals("FIXT.1.1")) {

                Path transportDataDictionary = Objects.requireNonNull(sessionSetting.getTransportDataDictionary(), () -> "TransportDataDictionary is null for session: " + sessionID);
                Path appDataDictionary = Objects.requireNonNull(sessionSetting.getAppDataDictionary(), () -> "AppDataDictionary is null for session: " + sessionID);

                Path pathToTransportDataDictionary = getPathToDictionary(temporaryDirectory, requireNotAbsolute(transportDataDictionary));
                Path pathToAppDataDictionary = getPathToDictionary(temporaryDirectory, requireNotAbsolute(appDataDictionary));

                sessionSetting.setTransportDataDictionary(requireFileExist(pathToTransportDataDictionary));
                sessionSetting.setAppDataDictionary(requireFileExist(pathToAppDataDictionary));
            } else {
                Path dataDictionary = Objects.requireNonNull(sessionSetting.getDataDictionary(), () -> "DataDictionary is null for session: " + sessionID);
                Path pathToDataDictionary = getPathToDictionary(temporaryDirectory, requireNotAbsolute(dataDictionary));
                sessionSetting.setDataDictionary(requireFileExist(pathToDataDictionary));
            }
        }

        MessageRouter<EventBatch> eventRouter = factory.getEventBatchRouter();
        MessageRouter<MessageGroupBatch> messageRouter = factory.getMessageRouterMessageGroupBatch();
        GrpcRouter grpcRouter = factory.getGrpcRouter();

        try {
            run(settings, messageRouter, eventRouter, grpcRouter, resources, factory.getBoxConfiguration().getBoxName());
        } catch (RuntimeError e) {
            LOGGER.error("Failed to start fix client", e);
        } catch (IncorrectDataFormat | CreatingConfigFileException e) {
            LOGGER.error("Error when using the config file", e);
        } catch (ConfigError e) {
            LOGGER.error("Failed to load file with session settings", e);
        } finally {
            System.exit(1);
        }

    }

    private static void writeDictionary(Path directory, InputStream dictionary, Path fileName) throws IOException, ConfigError {
        Path pathToDictionary = Files.createFile(getPathToDictionary(directory, fileName));
        Files.write(pathToDictionary, dictionary.readAllBytes());
        try {
            new DataDictionary(pathToDictionary.toString()); //check that xml file contains the correct values
        } catch (ConfigError error) {
            throw new ConfigError("Failed to create DataDictionary along the path " + fileName.getFileName(), error);
        }
    }

    private static Path requireNotAbsolute(Path path) {
        if (path.isAbsolute()) {
            throw new IllegalStateException("Dictionary path must not be absolute: " + path);
        }
        return path;
    }

    private static Path requireFileExist(Path pathToDictionary) {
        if (Files.notExists(pathToDictionary)) {
            throw new IllegalStateException("No dictionary along this path: " + pathToDictionary);
        }
        return pathToDictionary;
    }

    private static Path getPathToDictionary(Path dictionariesDirectory, Path dictionaryPath) {
        return dictionariesDirectory.resolve(dictionaryPath);
    }

    public static void run(Settings settings, MessageRouter<MessageGroupBatch> messageRouter, MessageRouter<EventBatch> eventRouter,
                           GrpcRouter grpcRouter, Deque<Resources> resources, String boxName) throws CreatingConfigFileException,
            ConfigError, IncorrectDataFormat, RuntimeError {

        File configFile = FixBeanUtil.createConfig(settings);

        Map<SessionID, ConnectionID> connectionIds = new HashMap<>();
        Map<String, SessionID> sessionIDs = settings.getSessionIDsByAliases();

        sessionIDs.forEach((sessionAlias, sessionId) -> {
            connectionIds.put(sessionId, ConnectionID.newBuilder().setSessionAlias(sessionAlias).build());
        });

        Event rootEvent = MessageRouterUtils.storeEvent(eventRouter, Event.start()
                        .name(boxName + " " + Instant.now() + " Sessions events")
                        .type("Microservice")
                        .bodyData(FixBeanUtil.getSessionTable(settings.getSessionSettings()))
                , null);
        String rootEventID = rootEvent.getId();

        LazyInitializer<Event> errorEventInitializer = new LazyInitializer<>() {
            @Override
            protected Event initialize() {
                return MessageRouterUtils.storeEvent(eventRouter, Event.start()
                                .name(boxName + " Error events " + Instant.now())
                                .type("Root error"),
                        rootEventID);
            }
        };

        Map<SessionID, String> sessionsEvents = new HashMap<>();
        sessionIDs.forEach((sessionAlias, sessionID) -> {
            String eventName = "Fix client " + sessionAlias + " " + Instant.now();
            Event event = MessageRouterUtils.storeEvent(eventRouter, rootEventID, eventName, "Microservice", null);
            sessionsEvents.put(sessionID, event.getId());
        });

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(THREADS_FOR_MESSAGES_AND_EVENTS_BATCHER);
        resources.add(new Resources("executor", executor::shutdown));

        MessageBatcher messageBatcher = new MessageBatcher(settings.maxBatchSize, settings.maxFlushTime, executor, (batch, direction) -> {
            try {
                messageRouter.send(batch, direction.name());
            } catch (IOException e) {
                LOGGER.error("Failed to send message batch.", e);
            }
            return Unit.INSTANCE;
        }, (e) -> {
            throw new IllegalStateException("Failed to send messages.", e);
        });

        EventBatcher eventBatcher = new EventBatcher(settings.maxBatchSize, settings.maxFlushTime, executor, eventBatch -> {
            try {
                eventRouter.sendAll(eventBatch, PUBLISH.getValue(), EVENT.getValue());
            } catch (IOException e) {
                LOGGER.error("Failed to send event batch: {}", e.getMessage());
            }
            return Unit.INSTANCE;
        });

        FixClient fixClient = new FixClient(new SessionSettings(configFile.getAbsolutePath()), settings, messageBatcher,
                eventBatcher, connectionIds, sessionsEvents, settings.queueCapacity);

        configFile.deleteOnExit();
        resources.add(new Resources("client", fixClient::stop));

        ClientController controller = new ClientController(fixClient);
        resources.add(new Resources("client-controller", controller::stop));

        MessageListener<MessageGroupBatch> listener = (consumerTag, groupBatch) -> {
            if (!controller.isRunning()) controller.start(settings.autoStopAfter);

            groupBatch.getGroupsList().forEach((group) -> {
                try {
                    if (group.getMessagesCount() != 1) {
                        if (LOGGER.isErrorEnabled()) {
                            LOGGER.error("Message group contains more or less than 1 message {} ", toJson(group));
                        }
                    } else {
                        AnyMessage message = group.getMessagesList().get(0);
                        if (!message.hasRawMessage()) {
                            if (LOGGER.isErrorEnabled()) {
                                LOGGER.error("Message in the group is not a raw message {} ", toJson(message));
                            }
                            return;
                        }
                        String sessionAlias = MessageUtil.getSessionAlias(message);
                        String strMessage = MessageUtil.rawToString(message);

                        SessionID sessionID = Objects.requireNonNull(sessionIDs.get(sessionAlias), () -> "Unknown session alias: " + sessionAlias);
                        Session session = Session.lookupSession(sessionID);

                        FixBean sessionSettings = FixBeanUtil.getSessionSettingsBySessionAlias(settings.getSessionSettings(), sessionAlias);
                        Objects.requireNonNull(sessionSettings, "Unknown session alias + " + sessionAlias);

                        FixMessage fixMessage;
                        FixMessageFactory messageFactory = (FixMessageFactory) session.getMessageFactory();
                        DataDictionary dataDictionary;
                        DataDictionary sessionDataDictionary;
                        if (sessionID.isFIXT()) {
                            dataDictionary = session
                                    .getDataDictionaryProvider()
                                    .getApplicationDataDictionary(session.getSenderDefaultApplicationVersionID());
                            sessionDataDictionary = session
                                    .getDataDictionaryProvider()
                                    .getSessionDataDictionary(sessionID.getBeginString());
                        } else {
                            dataDictionary = session.getDataDictionary();
                            sessionDataDictionary = dataDictionary;
                        }

                        if (sessionSettings.getOrderingFields() != null && sessionSettings.getOrderingFields().equals(YES_SETTING)) {
                            String msgType = MessageUtils.getMessageType(strMessage);

                            fixMessage = messageFactory.create(msgType, dataDictionary.getMsgFieldOrder(DataDictionary.HEADER_ID),
                                    dataDictionary.getMsgFieldOrder(msgType),
                                    dataDictionary.getMsgFieldOrder(DataDictionary.TRAILER_ID));

                            fixMessage.fromString(strMessage, sessionDataDictionary, dataDictionary, true);
                        } else {
                            fixMessage = new FixMessage(strMessage, sessionDataDictionary, dataDictionary);
                        }
                        dataDictionary.validate(fixMessage, true);

                        if (!message.getRawMessage().getParentEventId().getId().equals("")) {
                            fixMessage.setParentEventID(message.getRawMessage().getParentEventId());
                        }

                        if (!session.send(fixMessage)) {
                            throw new IllegalStateException("Message not sent. Message was not queued for transmission to the counterparty");
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("Failed to handle message group: {}", toJson(group), e);

                    AnyMessage message;
                    try {
                        message = group.getMessagesList().get(0);
                    } catch (Exception ex) {
                        message = null;
                    }
                    String parentEventID = null;
                    try {
                        parentEventID = MessageUtil.getParentEventID(message, errorEventInitializer.get().getId());
                    } catch (ConcurrentException ex) {
                        LOGGER.error("Failed to get root error event id", ex);
                    }
                    eventBatcher.onEvent(EventUtil.toEvent(parentEventID, "Failed to handle message group", e));
                }
            });
        };
        try {
            SubscriberMonitor monitor = Objects.requireNonNull(messageRouter.subscribe(listener, INPUT_QUEUE_ATTRIBUTE), "Subscriber monitor must not be null.");
            resources.add(new Resources("raw-monitor", monitor::unsubscribe));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to subscribe to input queue", e);
        }

        if (settings.autoStart) fixClient.start();
        if (settings.grpcStartControl) {
            try {
                GrpcServer grpcServer = new GrpcServer(grpcRouter.startServer(new ControlService(controller)));
                resources.add(new Resources("Grpc server", grpcServer::stop));
            } catch (IOException e) {
                LOGGER.error("Failed starting grpc server");
            }
        }

        LOGGER.info("Successfully started");

        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();
        resources.add(new Resources("await-shutdown", () -> {
            lock.lock();
            condition.signalAll();
            lock.unlock();
        }
        ));

        try {
            lock.lock();
            condition.await();
            lock.unlock();
        } catch (InterruptedException e) {
            LOGGER.error("Cannot get lock for Fix Client", e);
        }
        LOGGER.info("Finished running");
    }

    public static class Settings extends BaseFixBean {
        boolean grpcStartControl = false;
        boolean autoStart = true;
        int autoStopAfter = 0;
        int queueCapacity = 10000;
        boolean zipDictionaries = false;
        int maxBatchSize = 1000;
        long maxFlushTime = 1000L;
        @JsonProperty(required = true)
        List<FixBean> sessionSettings = new ArrayList<>();
        @JsonIgnore
        private Map<String, SessionID> sessionIDsByAliases = new HashMap<>();

        public Map<String, SessionID> getSessionIDsByAliases() {
            return sessionIDsByAliases;
        }

        public List<FixBean> getSessionSettings() {
            return sessionSettings;
        }

        public void setSessionSettings(List<FixBean> sessionSettings) throws IncorrectDataFormat {
            sessionIDsByAliases = new HashMap<>();    // if the session IDs or session aliases are not unique, we will get an error

            for (FixBean fixBean : sessionSettings) {
                SessionID sessionID = FixBeanUtil.getSessionID(fixBean);
                String sessionAlias = fixBean.getSessionAlias();

                if (sessionIDsByAliases.containsValue(sessionID) || sessionIDsByAliases.put(sessionAlias, sessionID) != null) {
                    throw new IncorrectDataFormat("SessionID and SessionAlias in sessions settings should be unique. " +
                            "Repeating of session alias: \"" + sessionAlias + "\" or sessionID: \"" + sessionID + "\"");
                }
            }
            this.sessionIDsByAliases = Collections.unmodifiableMap(sessionIDsByAliases);
            this.sessionSettings = Collections.unmodifiableList(sessionSettings);
        }

        public void setQueueCapacity(int queueCapacity) {
            if (queueCapacity < 0) {
                throw new IllegalArgumentException("Queue capacity cannot be negative (value of queue capacity: " + queueCapacity + ").");
            }
            this.queueCapacity = queueCapacity;
        }

        public boolean isGrpcStartControl() {
            return grpcStartControl;
        }

        public void setGrpcStartControl(boolean grpcStartControl) {
            this.grpcStartControl = grpcStartControl;
        }

        public boolean isAutoStart() {
            return autoStart;
        }

        public void setAutoStart(boolean autoStart) {
            this.autoStart = autoStart;
        }

        public int getAutoStopAfter() {
            return autoStopAfter;
        }

        public void setAutoStopAfter(int autoStopAfter) {
            if (autoStopAfter < 0) {
                throw new IllegalArgumentException("Timer for automatically stopping the client cannot be negative (value of timer: " + autoStopAfter + ").");
            }
            this.autoStopAfter = autoStopAfter;
        }

        public int getQueueCapacity() {
            return queueCapacity;
        }

        public boolean isZipDictionaries() {
            return zipDictionaries;
        }

        public void setZipDictionaries(boolean zipDictionaries) {
            this.zipDictionaries = zipDictionaries;
        }

        public int getMaxBatchSize() {
            return maxBatchSize;
        }

        public void setMaxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
        }

        public long getMaxFlushTime() {
            return maxFlushTime;
        }

        public void setMaxFlushTime(long maxFlushTime) {
            this.maxFlushTime = maxFlushTime;
        }

        @Override
        public String toString() {

            return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                    .appendSuper(super.toString())
                    .append("grpcStartControl", grpcStartControl)
                    .append("autoStart", autoStart)
                    .append("autoStopAfter", autoStopAfter)
                    .append("sessionsSettings", sessionSettings)
                    .append("sessionIDsByAliases", sessionIDsByAliases)
                    .append("zipDictionaries", zipDictionaries)
                    .toString();
        }
    }
}