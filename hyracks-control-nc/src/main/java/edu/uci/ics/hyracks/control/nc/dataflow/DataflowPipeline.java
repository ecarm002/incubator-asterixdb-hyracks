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
package edu.uci.ics.hyracks.control.nc.dataflow;

import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.task.IHyracksTask;
import edu.uci.ics.hyracks.control.nc.Stagelet;
import edu.uci.ics.hyracks.control.nc.runtime.OperatorRunnable;

public class DataflowPipeline implements IHyracksTask {
    private Stagelet stagelet;
    private OperatorRunnable runnable;

    public DataflowPipeline(OperatorRunnable runnable) {
        this.runnable = runnable;
    }

    public void run() throws HyracksException {

    }
}