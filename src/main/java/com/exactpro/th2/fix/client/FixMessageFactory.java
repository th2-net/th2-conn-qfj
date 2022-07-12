package com.exactpro.th2.fix.client;

import quickfix.Group;
import quickfix.Message;
import quickfix.MessageFactory;
import quickfix.MessageUtils;
import quickfix.field.ApplVerID;
import quickfix.field.MsgType;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static quickfix.FixVersions.BEGINSTRING_FIX40;
import static quickfix.FixVersions.BEGINSTRING_FIX41;
import static quickfix.FixVersions.BEGINSTRING_FIX42;
import static quickfix.FixVersions.BEGINSTRING_FIX43;
import static quickfix.FixVersions.BEGINSTRING_FIX44;
import static quickfix.FixVersions.BEGINSTRING_FIXT11;
import static quickfix.FixVersions.FIX50;
import static quickfix.FixVersions.FIX50SP1;
import static quickfix.FixVersions.FIX50SP2;

public class FixMessageFactory implements MessageFactory {

    private final Map<String, MessageFactory> messageFactories = new ConcurrentHashMap<>();
    private final ApplVerID defaultApplVerID;

    public FixMessageFactory() {
        this(ApplVerID.FIX50SP2);
    }

    public FixMessageFactory(String defaultApplVerID) {
        Objects.requireNonNull(defaultApplVerID, "defaultApplVerID is null");

        this.defaultApplVerID = new ApplVerID(defaultApplVerID);

        addFactory(BEGINSTRING_FIX40);
        addFactory(BEGINSTRING_FIX41);
        addFactory(BEGINSTRING_FIX42);
        addFactory(BEGINSTRING_FIX43);
        addFactory(BEGINSTRING_FIX44);
        addFactory(BEGINSTRING_FIXT11);
        addFactory(FIX50);
        addFactory(FIX50SP1);
        addFactory(FIX50SP2);
    }

    private void addFactory(String beginString) {
        String packageVersion = beginString.replace(".", "").toLowerCase();
        try {
            addFactory(beginString, "quickfix." + packageVersion + ".MessageFactory");
        } catch (ClassNotFoundException e) {
            // ignore - this factory is not available
        }
    }

    public void addFactory(String beginString, Class<? extends MessageFactory> factoryClass) {
        try {
            MessageFactory factory = factoryClass.newInstance();
            messageFactories.put(beginString, factory);
        } catch (Exception e) {
            throw new RuntimeException("can't instantiate " + factoryClass.getName(), e);
        }
    }

    public void addFactory(String beginString, String factoryClassName) throws ClassNotFoundException {
        // try to load the class
        Class<? extends MessageFactory> factoryClass = null;
        try {
            // try using our own classloader
            factoryClass = (Class<? extends MessageFactory>) Class.forName(factoryClassName);
        } catch (ClassNotFoundException e) {
            // try using context classloader (i.e. allow caller to specify it)
            Thread.currentThread().getContextClassLoader().loadClass(factoryClassName);
        }
        // if factory is found, add it
        if (factoryClass != null) {
            addFactory(beginString, factoryClass);
        }
    }

    @Override
    public Message create(String beginString, String msgType) {

        return create(beginString, defaultApplVerID, msgType);
    }


    public FixMessage create(String msgType, int[] headerFieldOrder, int[] bodyFieldOrder, int[] trailerFieldOrder) {

        FixMessage message = new FixMessage(headerFieldOrder, bodyFieldOrder, trailerFieldOrder);
        message.getHeader().setString(MsgType.FIELD, msgType);

        return message;
    }


    @Override
    public Group create(String beginString, String msgType, int correspondingFieldID) {
        MessageFactory messageFactory = messageFactories.get(beginString);
        if (messageFactory != null) {
            return messageFactory.create(beginString, msgType, correspondingFieldID);
        }
        throw new IllegalArgumentException("Unsupported FIX version: " + beginString);
    }

    @Override
    public Message create(String beginString, ApplVerID applVerID, String msgType) {
        MessageFactory messageFactory = messageFactories.get(beginString);
        if (beginString.equals(BEGINSTRING_FIXT11) && !MessageUtils.isAdminMessage(msgType)) {
            if (applVerID == null) {
                applVerID = new ApplVerID(defaultApplVerID.getValue());
            }
            messageFactory = messageFactories.get(MessageUtils.toBeginString(applVerID));
        }

        if (messageFactory != null) {
            return messageFactory.create(beginString, applVerID, msgType);
        }

        Message message = new Message();
        message.getHeader().setString(MsgType.FIELD, msgType);

        return message;
    }
}