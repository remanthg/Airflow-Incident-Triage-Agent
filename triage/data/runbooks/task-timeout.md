# Task time limits

## RB-201 | Task execution deadline

AirflowTaskTimeout, execution_timeout, an operator watchdog expiration, or a supervisor ending an operator at the configured wall-clock run limit indicates task execution time ran out. A task execution deadline reached after the configured elapsed duration is stronger evidence than a cleanup signal alone. Compare the attempt duration with the configured limit in the task definition.

## RB-202 | Request and queue delays

A remote API retry, a network socket timeout, or a wait for a free worker slot can occur before the task execution deadline. An optional cache miss during a retry can be incidental. Task termination needs evidence that the operator's own time limit was reached. Inspect elapsed duration and the preceding error to distinguish these symptoms.
