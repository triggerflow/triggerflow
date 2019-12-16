"""Health reporter.

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
"""

# https://pythonhosted.org/psutil/
import psutil

from datetime import datetime

MILLISECONDS_IN_SECOND = 1000
MEGABYTE = 10 ** 6
START_TIME = datetime.now()
CPU_INTERVAL = 0.5


def getSwapMemory():
    total, used, free, percent, sin, sout = psutil.swap_memory()
    swap_memory = {}
    swap_memory['total'] = '%d MB' % (total / MEGABYTE)
    swap_memory['used'] = '%d MB' % (used / MEGABYTE)
    swap_memory['free'] = '%d MB' % (free / MEGABYTE)
    swap_memory['percent'] = '%d MB' % (percent / MEGABYTE)
    swap_memory['sin'] = '%d MB' % (sin / MEGABYTE)
    swap_memory['sout'] = '%d MB' % (sout / MEGABYTE)

    return swap_memory


def getVirtualMemory():
    total, available, percent, used, free, active, inactive, buffers, cached, shared = psutil.virtual_memory()
    virtual_memory = {}
    virtual_memory['total'] = '%d MB' % (total / MEGABYTE)
    virtual_memory['available'] = '%d MB' % (available / MEGABYTE)
    virtual_memory['used'] = '%d MB' % (used / MEGABYTE)
    virtual_memory['free'] = '%d MB' % (free / MEGABYTE)
    virtual_memory['percent'] = '%d MB' % (percent / MEGABYTE)
    virtual_memory['active'] = '%d MB' % (active / MEGABYTE)
    virtual_memory['inactive'] = '%d MB' % (inactive / MEGABYTE)
    virtual_memory['buffers'] = '%d MB' % (buffers / MEGABYTE)
    virtual_memory['cached'] = '%d MB' % (cached / MEGABYTE)
    virtual_memory['shared'] = '%d MB' % (shared / MEGABYTE)

    return virtual_memory


def getCPUTimes():
    user, nice, system, idle, iowait, irq, softirq, steal, guest, guest_nice = psutil.cpu_times()
    cpu_times = {}
    cpu_times['user'] = '%d seconds' % user
    cpu_times['nice'] = '%d seconds' % nice
    cpu_times['system'] = '%d seconds' % system
    cpu_times['idle'] = '%d seconds' % idle
    cpu_times['iowait'] = '%d seconds' % iowait
    cpu_times['irq'] = '%d seconds' % irq
    cpu_times['softirq'] = '%d seconds' % softirq
    cpu_times['steal'] = '%d seconds' % steal
    cpu_times['guest'] = '%d seconds' % guest
    cpu_times['guest_nice'] = '%d seconds' % guest_nice

    return cpu_times


def getCPUPercent():
    return '%d%%' % psutil.cpu_percent(interval=CPU_INTERVAL)


def getDiskUsage():
    total, used, free, percent = psutil.disk_usage('/')
    disk_usage = {}
    disk_usage['total'] = '%d MB' % (total / MEGABYTE)
    disk_usage['used'] = '%d MB' % (used / MEGABYTE)
    disk_usage['free'] = '%d MB' % (free / MEGABYTE)
    disk_usage['percent'] = '%d MB' % (percent / MEGABYTE)

    return disk_usage


def getDiskIOCounters():
    read_count, write_count, read_bytes, write_bytes, read_time, write_time, read_merged_count, write_merged_count,\
        busy_time = psutil.disk_io_counters()
    disk_io_counters = {}
    disk_io_counters['read_count'] = read_count
    disk_io_counters['write_count'] = write_count
    disk_io_counters['read_bytes'] = '%d MB' % (read_bytes / MEGABYTE)
    disk_io_counters['write_bytes'] = '%d MB' % (write_bytes / MEGABYTE)
    disk_io_counters['read_time'] = '%d seconds' % (read_time / MILLISECONDS_IN_SECOND)
    disk_io_counters['write_time'] = '%d seconds' % (write_time / MILLISECONDS_IN_SECOND)
    disk_io_counters['read_merged_count'] = read_merged_count
    disk_io_counters['write_merged_count'] = write_merged_count
    disk_io_counters['busy_time'] = '%d seconds' % (busy_time / MILLISECONDS_IN_SECOND)

    return disk_io_counters


def getNetworkIOCounters():
    bytes_sent, bytes_recv, packets_sent, packets_recv, errin, errout, dropin, dropout = psutil.net_io_counters()
    net_io_counters = {}
    net_io_counters['bytes_sent'] = '%d MB' % (bytes_sent / MEGABYTE)
    net_io_counters['bytes_recv'] = '%d MB' % (bytes_recv / MEGABYTE)
    net_io_counters['packets_sent'] = packets_sent
    net_io_counters['packets_recv'] = packets_recv
    net_io_counters['errin'] = errin
    net_io_counters['errout'] = errout
    net_io_counters['dropin'] = dropin
    net_io_counters['dropout'] = dropout

    return net_io_counters


def getUpdateTime():
    currentTime = datetime.now()
    delta = currentTime - START_TIME
    uptimeSeconds = int(round(delta.total_seconds()))

    return '%d seconds' % uptimeSeconds


def getWorkers(consumers):
    consumerReports = []

    consumerCopyRO = consumers.getCopyForRead()
    for consumerId in consumerCopyRO:
        consumer = consumerCopyRO[consumerId]
        consumerInfo = {}
        consumerInfo[consumer.params['uuid']] = {
            'currentState': consumer.currentState(),
            'desiredState': consumer.desiredState(),
            'secondsSinceLastPoll': consumer.secondsSinceLastPoll(),
            'restartCount': consumer.restartCount()
        }
        consumerReports.append(consumerInfo)

    return consumerReports


def generateHealthReport(workers):
    healthReport = {}
    healthReport['uptime'] = getUpdateTime()
    healthReport['cpu_times'] = getCPUTimes()
    healthReport['cpu_percent'] = getCPUPercent()
    healthReport['virtual_memory'] = getVirtualMemory()
    healthReport['swap_memory'] = getSwapMemory()
    healthReport['disk_usage'] = getDiskUsage()
    healthReport['disk_io_counters'] = getDiskIOCounters()
    healthReport['net_io_counters'] = getNetworkIOCounters()
    healthReport['consumers'] = getWorkers(workers)

    return healthReport
