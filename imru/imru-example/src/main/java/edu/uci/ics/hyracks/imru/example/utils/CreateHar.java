/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.imru.example.utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class CreateHar {
    public static void copy(InputStream in, OutputStream out) throws IOException {
        byte[] bs = new byte[1024];
        while (true) {
            int len = in.read(bs);
            if (len <= 0)
                break;
            out.write(bs, 0, len);
        }
    }

    public static void copy(File file, OutputStream out) throws IOException {
        FileInputStream input = new FileInputStream(file);
        copy(input, out);
        input.close();
    }

    public static void add(String name, File file, ZipOutputStream zip) throws IOException {
        if (file.isDirectory()) {
            for (File f : file.listFiles())
                add(name.length() == 0 ? f.getName() : name + "/" + f.getName(), f, zip);
        } else {
            // System.out.println("add " + name);
            ZipEntry entry = new ZipEntry(name);
            entry.setTime(file.lastModified());
            zip.putNextEntry(entry);
            copy(file, zip);
            zip.closeEntry();
        }
    }

    public static void createHar(File harFile) throws IOException {
        File classPathFile = new File(".classpath");
        boolean startedFromEclipse = classPathFile.exists();
        ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(harFile));
        String p = CreateHar.class.getName().replace('.', '/') + ".class";
        URL url = CreateHar.class.getClassLoader().getResource(p);
        String path = url.getPath();
        if (startedFromEclipse) {
            path = path.substring(0, path.length() - p.length());
            Logger.getLogger(CreateHar.class.getName()).info("Add " + path + " to HAR");
            ByteArrayOutputStream memory = new ByteArrayOutputStream();
            ZipOutputStream zip2 = new ZipOutputStream(memory);
            add("", new File(path), zip2);
            zip2.finish();

            ZipEntry entry = new ZipEntry("lib/imru-example.jar");
            entry.setTime(System.currentTimeMillis());
            zip.putNextEntry(entry);
            zip.write(memory.toByteArray());
            zip.closeEntry();

            FileInputStream fileInputStream = new FileInputStream(classPathFile);
            byte[] buf = new byte[(int) classPathFile.length()];
            int start = 0;
            while (start < buf.length) {
                int len = fileInputStream.read(buf, start, buf.length - start);
                if (len < 0)
                    break;
                start += len;
            }
            fileInputStream.close();
            for (String line : new String(buf).split("\n")) {
                if (line.contains("kind=\"lib\"")) {
                    line = line.substring(line.indexOf("path=\""));
                    line = line.substring(line.indexOf("\"") + 1);
                    line = line.substring(0, line.indexOf("\""));
                    String name = line.substring(line.lastIndexOf("/") + 1);
                    add("lib/" + name, new File(line), zip);
                }
            }
        } else {
            String string = System.getProperty("java.class.path");
            if (string != null) {
                for (String s : string.split(File.pathSeparator)) {
                    if (s.length() == 0)
                        continue;
                    String name = s;
                    int t = name.lastIndexOf('/');
                    if (t > 0)
                        name = name.substring(t + 1);
                    if (new File(s).exists()) {
                        if (!(s.contains("jetty") && s.contains("6.1.14")))
                            add("lib/" + name, new File(s), zip);
                    }
                }
            }
        }
        zip.finish();
    }
}