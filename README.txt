FPGA enabling on YARN

Known issue:
1. if scheduler assign a FPGA slot that needs re-flashing which means it changed the container reuqest's FPGA slot's AFU_ID and Slot_ID. Sometime an issues happens that this modified container resource won't be re-cycled to RM.
