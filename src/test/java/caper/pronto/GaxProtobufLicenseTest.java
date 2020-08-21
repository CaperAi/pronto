package caper.pronto;


import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

import static org.junit.Assert.*;

public class GaxProtobufLicenseTest {
    @Test
    public void gaxProtobufLicenseIncluded() {
        try (InputStream is = getClass().getResourceAsStream("/licenses/GAX_PROTOBUF_LICENSE")) {
            try (Scanner sc = new Scanner(is)) {
                sc.hasNext();
                final String line = sc.nextLine();
                assertEquals("Copyright 2016, Google Inc. All rights reserved.", line.trim());
            }
        } catch (IOException e) {
            fail("Could not load GAX_PROTOBUF_LICENSE file. Redistributing this work violates the license terms for com.google.api.gax.protobuf.ProtoReflectionUtil");
        }
    }
}
