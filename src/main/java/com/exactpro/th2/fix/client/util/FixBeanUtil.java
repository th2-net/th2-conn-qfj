package com.exactpro.th2.fix.client.util;

import com.exactpro.th2.fix.client.Main.Settings;
import com.exactpro.th2.fix.client.exceptions.CreatingConfigFileException;
import com.exactpro.th2.fix.client.fixBean.FixBean;
import quickfix.IncorrectDataFormat;
import quickfix.SessionID;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FixBeanUtil {

    public static File createConfig(Settings settings) throws CreatingConfigFileException, IncorrectDataFormat {

        StringBuilder sb = new StringBuilder();

        sb.append(settings.toConfig("default"));

        for (FixBean fixBean : settings.getSessionsSettings()) {
            sb.append(fixBean.toConfig("session"));
        }

        File configFile;
        try {
            configFile = File.createTempFile("config", ".cfg");
            Files.writeString(Path.of(configFile.getAbsolutePath()), sb.toString());
        } catch (IOException e) {
            throw new CreatingConfigFileException("Failed to create a config file.", e);
        }

        return configFile;
    }

    public static <T> StringBuilder addToConfig(String tagName, T tagValue, StringBuilder sb) {

        if (tagValue != null) {
            sb.append(tagName)
                    .append("=")
                    .append(tagValue)
                    .append(System.lineSeparator());
        }
        return sb;
    }

    public static SessionID getSessionID(FixBean fixBean) {
        return new SessionID(fixBean.getBeginString(), fixBean.getSenderCompID(),
                fixBean.getSenderSubID(), fixBean.getSenderLocationID(), fixBean.getTargetCompID(),
                fixBean.getTargetSubID(), fixBean.getTargetLocationID(), fixBean.getSessionQualifier());
    }

}
