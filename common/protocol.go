package common

type Job struct {
	TableName string
	Env string
}

type JobEvent struct {
	EventType int
	Job *Job
}

func BuildJobObj(tablename,env string) *Job {
	return &Job{
		TableName: tablename,
		Env: env,
	}
}

func BuildJobEvent(eType int,jobObj *Job) *JobEvent {
	return &JobEvent{
		EventType: eType,
		Job: jobObj,
	}
}





