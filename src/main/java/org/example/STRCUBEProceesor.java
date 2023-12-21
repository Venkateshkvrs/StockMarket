package org.example;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;

public class STRCUBEProceesor {
    public static void main(String[] args) throws IOException, SQLException {
        String dimensionsDirectory = "./dimensions/";
        String factsDirectory = "./facts/";
        String xmlFileName = "DMInstance.xml";
        /* Sliding Window Implementation... */
        NewStreamProcessing nsp = new NewStreamProcessing();
        nsp.startStreamService(factsDirectory, dimensionsDirectory, xmlFileName);
    }
}
