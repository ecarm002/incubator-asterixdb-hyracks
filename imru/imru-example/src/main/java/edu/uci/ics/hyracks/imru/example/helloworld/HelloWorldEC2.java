package edu.uci.ics.hyracks.imru.example.helloworld;

import java.io.File;

import edu.uci.ics.hyracks.imru.example.utils.ImruEC2;

public class HelloWorldEC2 {
    public static void main(String[] args) throws Exception {
        File home = new File(System.getProperty("user.home"));
        File credentialsFile = new File(home, "AwsCredentials.properties");
        File privateKey = new File(home, "firstTestByRui.pem");
        File hyracksEc2Root = new File(home,
                "fullstack_imru/hyracks/hyracks-ec2/target/appassembler");
        String exampleData = System.getProperty("user.home")
                + "/fullstack_imru/imru/imru-example/data/helloworld";
        boolean setupClusterFirst = true;
        boolean uploadData = true;
        setupClusterFirst = false;
//        uploadData = false;
        int dataSplits = 5;
        String[] localPaths = new String[dataSplits];
        for (int i = 0; i < dataSplits; i++)
            localPaths[i] = exampleData + "/hello" + i + ".txt";
        ImruEC2 ec2 = new ImruEC2(credentialsFile, privateKey);
        if (setupClusterFirst)
            ec2.setup(hyracksEc2Root, 2, "t1.micro");
        String path;
        if (uploadData)
            path = ec2.uploadData(localPaths, "helloworld");
        else
            path = ec2.getSuggestedLocations(localPaths, "helloworld");
        String finalModel = ec2
                .run(new HelloWorldJob(), "", "helloworld", path);
        System.out.println("FinalModel: " + finalModel);
        System.exit(0);
    }
}
