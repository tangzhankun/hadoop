/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.fpga;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class FpgaResourceAllocator {

    static final Log LOG = LogFactory.getLog(FpgaResourceAllocator.class);

    private final static String RESOURCE_TYPE = "fpga";
    private Set<Integer> allowedFpgas = new TreeSet<>();
    //key is resource type
    private LinkedHashMap<String, List<FpgaDevice>> availableFpga = new LinkedHashMap<>();

    //key is requetor
    private LinkedHashMap<String, List<FpgaDevice>> usedFpgaByRequestor = new LinkedHashMap<>();

    private Context nmContext;

    @VisibleForTesting
    public HashMap<String, List<FpgaDevice>> getAvailableFpga() {
        return availableFpga;
    }

    public FpgaResourceAllocator(Context ctx) {
        this.nmContext = ctx;
    }

    @VisibleForTesting
    public int getAvailableFpgaCount() {
        int count = 0;
        for (List<FpgaDevice> l : availableFpga.values()) {
            count += l.size();
        }
        return count;
    }
    @VisibleForTesting
    public HashMap<String, List<FpgaDevice>> getUsedFpga() {
        return usedFpgaByRequestor;
    }

    @VisibleForTesting
    public int getUsedFpgaCount() {
        int count = 0;
        for (List<FpgaDevice> l : usedFpgaByRequestor.values()) {
            count += l.size();
        }
        return count;
    }
    public static class FpgaAllocation {

        private List<FpgaDevice> allowed = Collections.emptyList();

        private List<FpgaDevice> denied = Collections.emptyList();

        FpgaAllocation(List<FpgaDevice> allowed, List<FpgaDevice> denied) {
            if (allowed != null) {
                this.allowed = ImmutableList.copyOf(allowed);
            }
            if (denied != null) {
                this.denied = ImmutableList.copyOf(denied);
            }
        }

        public List<FpgaDevice> getAllowed() {
            return allowed;
        }

        public List<FpgaDevice> getDenied() {
            return denied;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("\nFpgaAllocation\n\tAllowed:\n");
            for (FpgaDevice device : allowed) {
                sb.append("\t\t");
                sb.append(device + "\n");
            }
            sb.append("\tDenied\n");
            for (FpgaDevice device : denied) {
                sb.append("\t\t");
                sb.append(device + "\n");
            }
            return sb.toString();
        }
    }

    static class FpgaDevice implements Comparable<FpgaDevice>{

        public String getType() {
            return type;
        }

        public Integer getMajor() {
            return major;
        }

        public Integer getMinor() {
            return minor;
        }

        public String getIPID() {
            return IPID;
        }

        public void setIPID(String IPID) {
            this.IPID = IPID;
        }

        private String type;
        private Integer major;
        private Integer minor;
        private String IPID;

        FpgaDevice(String type, Integer major, Integer minor, String IPID) {
            this.type = type;
            this.major = major;
            this.minor = minor;
            this.IPID = IPID;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof FpgaDevice)) {
                return false;
            }
            FpgaDevice other = (FpgaDevice)obj;
            if (other.getType() == this.type &&
                    other.getMajor() == this.major &&
                    other.getMinor() == this.minor) {
                return true;
            }
            return false;
        }

        @Override
        public int compareTo(FpgaDevice o) {
            return 0;
        }

        @Override
        public String toString() {
            return "FPGAdevice:(Type: " + this.type + ",Major: " + this.major + ", Minor: " + this.minor + ",IPID:" + this.IPID + ")";
        }
    }

    public synchronized void addFpga(String type, int majorNumber, int minorNumber, String IPID) {
        allowedFpgas.add(minorNumber);
        FpgaDevice newDevice = new FpgaDevice(type, majorNumber, minorNumber, IPID);
        if (availableFpga.get(type) == null) {
            List<FpgaDevice> list = new LinkedList<>();
            list.add(newDevice);
            availableFpga.put(type,list);
        } else {
            availableFpga.get(type).add(newDevice);
        }
        LOG.info("Add a FPGADevice: " + newDevice);
    }

    public synchronized void updateFpga(String requestor, FpgaAllocation allocation, String newIPID) {
        List<FpgaDevice> usedFpgas = usedFpgaByRequestor.get(requestor);
        List<FpgaDevice> reconfiguredFpgas = allocation.getAllowed();
        int index;
        for (FpgaDevice device : reconfiguredFpgas) {
            index = findMatchedFpga(usedFpgas, device);
            if (-1 != index) {
                usedFpgas.get(index).setIPID(newIPID);
            } else {
                LOG.warn("unknown reason that no record for this allocated device:" + device);
            }
        }
    }

    private synchronized int findMatchedFpga(List<FpgaDevice> devices, FpgaDevice item) {
        int i = 0;
        for (; i < devices.size(); i++) {
            if (devices.get(i) == item) {
                return i;
            }
        }
        return -1;
    }

    public synchronized FpgaAllocation assignFpga(String type, long count, ContainerId containerId, String preference) throws ResourceHandlerException {
        List<FpgaDevice> currentAvailableFpga = availableFpga.get(type);
        String requestor = containerId.toString();
        if (null == currentAvailableFpga) {
            LOG.warn("No such type of resource available: " + type);
        }
        if (count <= 0 || count > currentAvailableFpga.size()) {
            LOG.warn("Invalid request count or no enough FPGA:" + count + ", available:" + getAvailableFpgaCount());
            return null;
        }
        List<FpgaDevice> assignedFpgas = new ArrayList<>();
        int matchIPCount = 0;
        for (FpgaDevice device : currentAvailableFpga) {
            if (device.getIPID() == preference) {
                assignedFpgas.add(device);
                currentAvailableFpga.remove(device);
                matchIPCount++;
            }
        }
        int remaining = (int)count - matchIPCount;
        while (remaining > 0) {
            assignedFpgas.add(currentAvailableFpga.remove(0));
            remaining--;
        }

        if (null == usedFpgaByRequestor.get(requestor)) {
            usedFpgaByRequestor.put(requestor, assignedFpgas);
        } else {
            usedFpgaByRequestor.get(requestor).addAll(assignedFpgas);
        }

        // Record in state store if we allocated anything
        if (!assignedFpgas.isEmpty()) {
            List<Serializable> allocatedDevices = new ArrayList<>();
            for (FpgaDevice fpga : assignedFpgas) {
                allocatedDevices.add(String.valueOf(fpga.getMinor()));
            }
            try {
                nmContext.getNMStateStore().storeAssignedResources(containerId,
                        RESOURCE_TYPE, allocatedDevices);
            } catch (IOException e) {
                throw new ResourceHandlerException(e);
            }
        }
        return new FpgaAllocation(assignedFpgas, currentAvailableFpga);
    }

    public synchronized void recoverAssignedFpgas(ContainerId containerId) throws ResourceHandlerException {
        Container c = nmContext.getContainers().get(containerId);
        if (null == c) {
            throw new ResourceHandlerException(
                    "This shouldn't happen, cannot find container with id="
                            + containerId);
        }

        for (Serializable deviceId : c.getResourceMappings().getAssignedResources(
                RESOURCE_TYPE)){
            if (!(deviceId instanceof String)) {
                throw new ResourceHandlerException(
                        "Trying to recover device id, however it"
                                + " is not String, this shouldn't happen");
            }

            int devId = Integer.parseInt((String)deviceId);

            // Make sure it is in allowed GPU device.
            if (!allowedFpgas.contains(devId)) {
                throw new ResourceHandlerException("Try to recover device id = " + devId
                        + " however it is not in allowed device list:" + StringUtils
                        .join(",", allowedFpgas));
            }

            // Make sure it is not occupied by anybody else

            if (!getAvailableFpga().get("opencl").contains(devId)) {
                throw new ResourceHandlerException("Try to recover device id = " + devId
                        + " however it is already assigned to others");
            }

            getUsedFpga().get("opencl").add(new FpgaDevice("opencl",-1,devId,null));
        }
    }

    public synchronized void cleanupAssignFpgas(String requestor) {
        List<FpgaDevice> usedFpgas = usedFpgaByRequestor.get(requestor);
        if(usedFpgas != null) {
            for (FpgaDevice device : usedFpgas) {
                availableFpga.get(device.getType()).add(device);//add back to availableFpga
            }
            usedFpgaByRequestor.remove(requestor);
        }
    }

}
