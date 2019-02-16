// This is a POC for a worker group abstraction that will take any func()error
// and throws that work to the group of workers. A Queue(worker queue) type is used to
// orchestrate the madness. You can safely call it concurrently and it'll
// throw whatever work is "Add"ed to the next available worker. The workers
// register themselves in the Queue's worker queue themselves, so the Queue
// blocks until a worker is available when calling Add. That is unless you
// were to use a buffered work stream, which allows you to add back pressure
// to the work the workers do to the size of that buffered queue. Would greatly
// appreciate any and all feedback.  Thanks!
package workit
