# '''
# CS5250 Assignment 4, Scheduling policies simulator
# Sample skeleton program
# Input file:
#     input.txt
# Output files:
#     FCFS.txt
#     RR.txt
#     SRTF.txt
#     SJF.txt
# '''
import sys
import logging
# change logging level from INFO to DEBUG to print debugging logs
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(funcName)s - %(lineno)d - %(message)s')

# input_file = 'test_input.txt'
input_file = 'input.txt'


class Process:
    last_scheduled_time = 0

    def __init__(self, pid, arrive_time, burst_time):
        self.pid = pid
        self.arrive_time = arrive_time
        self.burst_time = burst_time
        self.remaining_time = burst_time
        self.departure_time = -1
        self.burst_pd = 5  # initial guess of burst size = 5

    # for printing purpose
    def __repr__(self):
        # return '[pid %d : arrival_time %d,  burst_time %d]' % (self.pid, self.arrive_time, self.burst_time)
        return "[pid {}: arrive = {}, burst_time = {}, remaining_time = {}, departure = {}, burst_pd = {}]".\
            format(self.pid, self.arrive_time, self.burst_time, self.remaining_time, self.departure_time, self.burst_pd)

    def reset(self):
        self.remaining_time = self.burst_time
        self.departure_time = -1
        self.burst_pd = 5


class Queue:
    def __init__(self, qid, process_list):
        self.qid = qid
        self.queue = []

        # reverse the process list so that the queue is FIFO
        for p in process_list[::-1]:
            if p.pid == qid:
                self.queue.append(p)

        self.length = len(self.queue)

    def is_non_empty(self):
        return len(self.queue) > 0

    def print_q(self):
        print("===========================================================================")
        print("Queue {}: length = {}, is_non_empty = {}".format(self.qid, self.length, self.is_non_empty()))
        for p in self.queue:
            print(p)
        print("===========================================================================")


def FCFS_scheduling(process_list):
    # to store the (switching time, process_id) pair
    schedule = []
    current_time = 0
    waiting_time = 0

    for process in process_list:
        if current_time < process.arrive_time:
            current_time = process.arrive_time

        schedule.append((current_time, process.pid))
        current_time += process.burst_time
        process.remaining_time = 0
        process.departure_time = current_time
        waiting_time += process.departure_time - process.arrive_time - process.burst_time

    # print("FCFS: completion time = {}".format(current_time))
    average_waiting_time = waiting_time/float(len(process_list))
    print("FCFS: avg waiting time = {:.2f}".format(average_waiting_time))

    return schedule, average_waiting_time


# ===========================================================================================
# RR: Round Robin
# ------------------
# Input: process_list, time_quantum (Positive Integer)
# Output_1 : Schedule list contains pairs of (time_stamp, process_id) indicating the time switching to that proccess_id
# Output_2 : Average Waiting Time
#
# Assumptions:
#       1. All tasks are CPU bound
#       2. All time parameters are using the minimum time unit. i.e.
#           •	The minimum time advancement is 1 unit.
#           •	All CPU bursts arrive and departure at integer time
#           •	All burst sizes are integer values
#           •	Context switching only happens at integer time
#       3. Context switching overhead is 0.
#       4. The scheduler goes around processes following process IDs in ascending order,
#          starting from the smallest process ID.
#          e.g. [4 processes] 0 -> 1 -> 2 -> 3 ->0
# ===========================================================================================
def RR_scheduling(process_list, time_quantum ):
    # to store the (switching time, process_id) pair
    schedule = []
    current_time = 0
    waiting_time = 0
    current_p = -1  # save the current process in processing, use -1 as default value for initialization

    processes = list(set(p.pid for p in process_list))  # list of distinct processes
    p_count = len(processes)         # total number of processes
    # print("Total number of input = {}; total number of processes = {}".format(len(process_list), p_count))

    # create queues to hold inputs according to process id
    queues = []
    for i in range(p_count):
        queues.append(Queue(processes[i], process_list))
        # queues[i].print_q()

    # continue processing while any queue is non-empty
    while sum(q.is_non_empty() for q in queues):
        count = 0  # keep track of number of queues to be processed at current_time

        # check each queue in sequence
        for i in range(p_count):
            if queues[i].is_non_empty():
                p = queues[i].queue[-1]
                if current_time >= p.arrive_time:
                    count += 1
                    if current_p != p.pid:
                        schedule.append((current_time, p.pid))  # record the process switching
                        current_p = p.pid

                    if p.remaining_time <= time_quantum:
                        current_time += p.remaining_time  # advance current time to the completion of current burst
                        p.remaining_time = 0
                        p.departure_time = current_time
                        waiting_time += p.departure_time - p.arrive_time - p.burst_time  # accumulate waiting time
                        queues[i].queue.pop()  # remove the burst from the queue
                    else:  # current burst can't be completed within this quantum
                        current_time += time_quantum
                        p.remaining_time -= time_quantum  # update remaining burst time

        if count == 0:
            # there's no process in the queues at current_time
            current_time += 1   # advance current time by 1 unit

    # print("RR: completion time = {}".format(current_time))
    average_waiting_time = waiting_time/float(len(process_list))
    print("RR: avg waiting time = {:.2f}".format(average_waiting_time))

    return schedule, average_waiting_time


# ===========================================================================================
# SRTF: Shortest Remaining Time First [preemptive]
# --------------------------------------------------
# Input: process_list
# Output_1 : Schedule list contains pairs of (time_stamp, process_id) indicating the time switching to that proccess_id
# Output_2 : Average Waiting Time
#
# Assumptions:
#       1. All tasks are CPU bound
#       2. All time parameters are using the minimum time unit. i.e.
#           •	The minimum time advancement is 1 unit.
#           •	All CPU bursts arrive and departure at integer time
#           •	All burst sizes are integer values
#           •	Context switching only happens at integer time
#       3. Context switching overhead is 0.
#       4. If 2 CPU bursts have the same remaining time, the one that arrives earlier gets scheduled first
# ===========================================================================================
def SRTF_scheduling(process_list):
    # to store the (switching time, process_id) pair
    schedule = []
    waiting_time = 0
    current_p = -1  # save the current process in processing, use -1 as default value for initialization

    def find_srt(p_list, current_t):
        # find the job with shortest remaining time (srt) at current time
        candidates = []
        while sum(p1.remaining_time for p1 in p_list) > 0:  # there is still unfinished CPU burst
            for p1 in p_list:
                if p1.arrive_time <= current_t and p1.remaining_time > 0:
                    candidates.append(p1)

            # if candidates:
            #     srt = min(p2.remaining_time for p2 in candidates)
            #     for p2 in candidates:
            #         if p2.remaining_time == srt:
            #             return p2, current_t

            if candidates:
                sp = sorted(candidates, key=lambda x: (x.remaining_time, x.arrive_time))
                return sp[0], current_t
            else:  # no candidate, advance time by 1 unit to continue the search
                current_t += 1

    for i in range(len(process_list)):
        current_time = process_list[i].arrive_time

        if i == len(process_list) - 1:
            # since no more arrival event is scheduled, set the next arrival to the max value (int)
            next_arrival = sys.maxsize
            logging.info("Set next arrival after last arrival to {}".format(next_arrival))
        else:
            next_arrival = process_list[i + 1].arrive_time

        while current_time < next_arrival:  # before next arrival
            p, current_time = find_srt(process_list, current_time)
            if current_p != p.pid:
                schedule.append((current_time, p.pid))  # record the process switching
                current_p = p.pid

            if p.remaining_time <= next_arrival - current_time:
                # advance current time to completion of current burst only and then look for next candidate
                current_time += p.remaining_time
                p.remaining_time = 0
                p.departure_time = current_time
                waiting_time += p.departure_time - p.arrive_time - p.burst_time
            else:
                p.remaining_time -= (next_arrival - current_time)
                current_time = next_arrival

            if sum(p.remaining_time for p in process_list) == 0:
                # print("SRTF: completion time = {}".format(current_time))
                break

    average_waiting_time = waiting_time/float(len(process_list))
    print("SRTF: avg waiting time = {:.2f}".format(average_waiting_time))

    return schedule, average_waiting_time


# ===========================================================================================
# SJF: Shortest Job First [non-preemptive]
# --------------------------------------------------
# Input: process_list, alpha (0<= alpha <=1)
# Output_1 : Schedule list contains pairs of (time_stamp, process_id) indicating the time switching to that proccess_id
# Output_2 : Average Waiting Time
#
# Assumptions:
#       1. All tasks are CPU bound
#       2. All time parameters are using the minimum time unit. i.e.
#           •	The minimum time advancement is 1 unit.
#           •	All CPU bursts arrive and departure at integer time
#           •	All burst sizes are integer values
#           •	Context switching only happens at integer time
#       3. Context switching overhead is 0.
#       4a. If 2 CPU bursts have the same prediction, the one arrives earlier gets scheduled first
#           * this approach assumes that the scheduler doesn't know the burst size, it has to rely on the prediction
#       4b. If 2 CPU bursts have the same prediction, the one with smaller burst size gets scheduled first
#           If both prediction and burst size are the same, the one arrives earlier gets scheduled first
#           * this approach combines SJF and SRTF, which gives more optimal results compared to 4a.
# ===========================================================================================
def SJF_scheduling(process_list, alpha):
    # to store the (switching time, process_id) pair
    schedule = []
    current_time = 0
    waiting_time = 0
    current_p = -1  # save the current process in processing, use -1 as default value for initialization

    calc_burst_pd(process_list, alpha)

    def find_sj(p_list, current_t):
        # find the shortest job (minimum burst prediction) at current time
        candidates = []
        while sum(p1.remaining_time for p1 in p_list) > 0:  # there is still unfinished CPU burst
            for p1 in p_list:
                if p1.arrive_time <= current_t and p1.remaining_time > 0:
                    candidates.append(p1)

            # if candidates:
            #     sj = min(p2.burst_pd for p2 in candidates)
            #     candidates_r2 = []
            #     for p2 in candidates:
            #         if p2.burst_pd == sj:
            #             candidates_r2.append(p2)
            #
            #     if len(candidates_r2) > 1:
            #         bt = min(p3.burst_time for p3 in candidates_r2)
            #         for p3 in candidates_r2:
            #             if p3.burst_time == bt:
            #                 return p3, current_t
            #     else:
            #         return candidates_r2[0], current_t

            if candidates:
                # 4a. assumes that the scheduler doesn't know the burst_time
                sp = sorted(candidates, key=lambda x: (x.burst_pd, x.arrive_time))
                # 4b. combine SJF and SRTF. Uncomment the line below to enable 4b.
                # sp = sorted(candidates, key=lambda x: (x.burst_pd, x.burst_time, x.arrive_time))
                return sp[0], current_t

            else:  # no candidate, advance time by 1 unit to continue the search
                current_t += 1

    for p in process_list:
        while p.remaining_time > 0:
            if current_time <= p.arrive_time:
                current_time = p.arrive_time

            q, current_time = find_sj(process_list, current_time)
            if current_p != q.pid:
                schedule.append((current_time, q.pid))  # record the process switching
                current_p = q.pid

            # advance current time to completion of current burst
            current_time += q.burst_time
            q.remaining_time = 0
            q.departure_time = current_time
            waiting_time += q.departure_time - q.arrive_time - q.burst_time

    # print("SJF: completion time = {}".format(current_time))
    average_waiting_time = waiting_time/float(len(process_list))
    print("SJF: avg waiting time = {:.2f}".format(average_waiting_time))

    return schedule, average_waiting_time


def calc_burst_pd(p_list, alpha):
    for i in range(len(p_list)):
        for j in range(i - 1, -1, -1):
            if p_list[j].pid == p_list[i].pid:  # find the previous CPU burst of the same process
                p_list[i].burst_pd = alpha * p_list[j].burst_time + (1 - alpha) * p_list[j].burst_pd
                break


def read_input():
    result = []
    with open(input_file) as f:
        for line in f:
            array = line.split()
            if len(array) != 3:
                print("wrong input format")
                exit()
            result.append(Process(int(array[0]), int(array[1]), int(array[2])))
    return result


def write_output(file_name, schedule, avg_waiting_time):
    with open(file_name,'w') as f:
        for item in schedule:
            f.write(str(item) + '\n')
        f.write('average waiting time %.2f \n' % avg_waiting_time)


def main():
    process_list = read_input()

    print("printing input -------------------------------------------------------------")
    for p in process_list:
        print(p)

    print("simulating FCFS ------------------------------------------------------------")
    FCFS_schedule, FCFS_avg_waiting_time = FCFS_scheduling(process_list)
    write_output('FCFS.txt', FCFS_schedule, FCFS_avg_waiting_time)

    for p in process_list:
        print(p)
        p.reset()

    print ("simulating RR --------------------------------------------------------------")
    RR_schedule, RR_avg_waiting_time = RR_scheduling(process_list, time_quantum=2)
    write_output('RR.txt', RR_schedule, RR_avg_waiting_time)

    for p in process_list:
        print(p)
        p.reset()

    print ("simulating SRTF ------------------------------------------------------------")
    SRTF_schedule, SRTF_avg_waiting_time = SRTF_scheduling(process_list)
    write_output('SRTF.txt', SRTF_schedule, SRTF_avg_waiting_time)

    for p in process_list:
        print(p)
        p.reset()

    print ("simulating SJF -------------------------------------------------------------")
    SJF_schedule, SJF_avg_waiting_time = SJF_scheduling(process_list, alpha=0.5)
    write_output('SJF.txt', SJF_schedule, SJF_avg_waiting_time)

    for p in process_list:
        print(p)
        p.reset()


if __name__ == '__main__':
    main()

