# Barrier

<p style="text-align: left">
  <a href="#русский">Русский</a> ・ <a href="#english">English</a>
</p>

---

## Русский

Barrier — точка встречи для фиксированного числа горутин. Каждая горутина доходит до барьера и останавливается. Продолжить работу они могут только после того, как до той же точки дошли все остальные участники.

Реализация в этом пакете делит один цикл работы на три части:

1. подготовка до вызова `Before`;
2. параллельная работа между `Before` и `After`;
3. следующий этап после `After`.

`Before` не выпускает ни одну горутину, пока у входа не соберутся все `size` участников. `After` делает то же самое на выходе: горутина, закончившая середину цикла первой, ждёт остальных. После выхода из `After` барьер снова пуст и может использоваться в следующем цикле.

### Как это устроено

`NewBarrier(size int) *Barrier` создаёт барьер на строго определённое число участников. Значение `size` не меняется во время работы. Текущая реализация ожидает положительное значение; при `size <= 0` вызов `Before` заблокируется навсегда.

`Before()` под мьютексом увеличивает внутренний счётчик `count`. Последний участник, для которого `count` становится равен `size`, отправляет `size` сигналов в `beforeCh`. После этого каждый участник получает по одному сигналу и выходит из метода.

`After()` уменьшает тот же счётчик. Когда последний участник доводит `count` до нуля, метод отправляет `size` сигналов в `afterCh`. Все горутины завершают текущий цикл, а состояние барьера возвращается к исходному.

Пара `Before`/`After` обязательна. Если хотя бы одна горутина вернётся по ошибке, запаникует или пропустит один из вызовов, остальные участники будут ждать бесконечно. В API нет `context.Context`, тайм-аута или способа исключить выбывшего участника.

### Как вызывать

Ниже один процесс валидатора проверяет блок тремя горутинами. Каждая готовит свою часть блока, после `Before` проверяет транзакции, а после `After` читает общий результат и формирует голос. Ни одна горутина не голосует по частично проверенному блоку.

```go
func ExampleBlockchainBarrier() {
	const workers = 3

	block := []string{
		"alice->bob:4",
		"bob->carol:2",
		"carol->dave:1",
		"dave->eve:3",
		"eve->alice:2",
		"alice->carol:1",
	}

	barrier := NewBarrier(workers)
	partitionValid := make([]bool, workers)
	votes := make([]bool, workers)

	var wg sync.WaitGroup
	wg.Add(workers)

	for workerID := 0; workerID < workers; workerID++ {
		go func(id int) {
			defer wg.Done()

			// Preparation is local: assign every third transaction to this worker.
			partition := make([]string, 0)
			for txIndex := id; txIndex < len(block); txIndex += workers {
				partition = append(partition, block[txIndex])
			}

			// Nobody starts validation until every partition is ready.
			barrier.Before()

			valid := true
			for _, tx := range partition {
				if tx == "" {
					valid = false
				}
			}
			partitionValid[id] = valid

			// Nobody votes until every partition has been validated.
			barrier.After()

			blockValid := true
			for _, result := range partitionValid {
				blockValid = blockValid && result
			}
			votes[id] = blockValid
		}(workerID)
	}

	wg.Wait()

	for _, vote := range votes {
		if !vote {
			fmt.Println("block rejected")
			return
		}
	}
	fmt.Println("block accepted")
}
```

Что происходит в примере:

1. Транзакции делятся между тремя горутинами. Подготовка частей может закончиться в разное время.
2. `Before` фиксирует границу начала проверки: быстрый воркер не начинает работать со своей частью, пока два других ещё формируют свои наборы.
3. Каждый воркер записывает результат только в свой элемент `partitionValid`, поэтому конкурентной записи в одну ячейку нет.
4. `After` фиксирует границу завершения проверки. После него все элементы `partitionValid` уже записаны и больше не меняются.
5. Каждый воркер видит итог проверки всего блока и формирует голос. В реальном приложении здесь может быть построение результата исполнения, подпись голоса или переход к следующей высоте.

Это пример синхронизации стадий **внутри одного узла**. Для координации отдельных блокчейн-узлов, работающих в разных процессах или на разных машинах, нужны сетевой протокол, тайм-ауты и правила консенсуса. In-memory barrier эту задачу не решает.

### Операции

| Функция | Сигнатура | Что делает |
|---|---|---|
| `NewBarrier` | `func NewBarrier(size int) *Barrier` | создаёт барьер для фиксированного числа участников |
| `Before` | `func (b *Barrier) Before()` | ждёт, пока все участники войдут в текущий цикл |
| `After` | `func (b *Barrier) After()` | ждёт, пока все участники закончат параллельную часть цикла |

### Когда использовать

Барьер уместен, когда одна бизнес-операция разбита на повторяющиеся фазы и следующая фаза не имеет права начаться раньше завершения предыдущей у всех исполнителей. Например:

- расчёт риска по нескольким портфелям, после которого строится общий отчёт;
- параллельная обработка сегментов изображения перед сборкой кадра;
- симуляция по шагам, где новый такт использует состояние, рассчитанное всеми участниками на предыдущем такте;
- проверка частей блока перед вычислением общего результата или формированием голоса;
- пакетная загрузка данных, после которой все воркеры должны одновременно перейти к обработке.

Бизнес-инвариант здесь важнее самого механизма: данные текущей фазы должны быть полностью готовы до того, как кто-либо начнёт следующую. Если такой границы в задаче нет, барьер только добавит ожидание.

Не используйте этот `Barrier`, если число участников может меняться, отдельную операцию можно отменить, воркер способен завершиться раньше остальных или внешняя система может зависнуть. В этих случаях нужен протокол с `context.Context`, тайм-аутом и явной обработкой отказов. Для одноразового ожидания завершения набора горутин проще взять `sync.WaitGroup`; для защиты общей структуры данных нужен `sync.Mutex`; для ограничения числа одновременно выполняемых операций — семафор или worker pool.

### Правила использования

- Передавайте в `NewBarrier` число горутин, которые действительно будут участвовать в **каждом** цикле.
- В каждой горутине вызывайте методы в одном порядке: сначала `Before`, затем работа, затем `After`.
- Не делайте `return` между `Before` и `After`. Ошибку сохраните в отдельный результат, вызовите `After`, а решение о прекращении работы принимайте после барьера.
- Не запускайте следующий цикл, пропустив `After` предыдущего.
- Не вызывайте `After` без соответствующего `Before`.
- Запускайте конкурентные тесты с `go test -race ./...`.

---

## English

A barrier is a meeting point for a fixed number of goroutines. Each goroutine reaches the barrier and stops. They may continue only after every other participant has reached the same point.

The implementation in this package splits one work cycle into three parts:

1. preparation before `Before`;
2. parallel work between `Before` and `After`;
3. the next stage after `After`.

`Before` releases no goroutine until all `size` participants are waiting at the entrance. `After` applies the same rule at the exit: a goroutine that finishes the middle section first waits for the rest. Once every participant leaves `After`, the barrier is empty and ready for another cycle.

### How it works

`NewBarrier(size int) *Barrier` creates a barrier for an exact number of participants. The value of `size` cannot change while the barrier is in use. This implementation expects a positive value; with `size <= 0`, a call to `Before` blocks forever.

`Before()` increments the internal `count` while holding a mutex. The last participant, which makes `count` equal to `size`, sends `size` signals to `beforeCh`. Each participant receives one signal and returns from the method.

`After()` decrements the same counter. When the last participant brings `count` back to zero, it sends `size` signals to `afterCh`. Every goroutine can then finish the current cycle, and the barrier returns to its initial state.

The `Before`/`After` pair is mandatory. If one goroutine returns on an error, panics, or skips either call, every other participant can wait forever. The API has no `context.Context`, timeout, or way to remove a failed participant.

### How to call it

The example below uses three goroutines inside one validator process to check a block. Each goroutine prepares its block partition, validates transactions after `Before`, then reads the combined result and produces a vote after `After`. No goroutine votes on a partially checked block.

```go
func ExampleBlockchainBarrier() {
	const workers = 3

	block := []string{
		"alice->bob:4",
		"bob->carol:2",
		"carol->dave:1",
		"dave->eve:3",
		"eve->alice:2",
		"alice->carol:1",
	}

	barrier := NewBarrier(workers)
	partitionValid := make([]bool, workers)
	votes := make([]bool, workers)

	var wg sync.WaitGroup
	wg.Add(workers)

	for workerID := 0; workerID < workers; workerID++ {
		go func(id int) {
			defer wg.Done()

			// Preparation is local: assign every third transaction to this worker.
			partition := make([]string, 0)
			for txIndex := id; txIndex < len(block); txIndex += workers {
				partition = append(partition, block[txIndex])
			}

			// Nobody starts validation until every partition is ready.
			barrier.Before()

			valid := true
			for _, tx := range partition {
				if tx == "" {
					valid = false
				}
			}
			partitionValid[id] = valid

			// Nobody votes until every partition has been validated.
			barrier.After()

			blockValid := true
			for _, result := range partitionValid {
				blockValid = blockValid && result
			}
			votes[id] = blockValid
		}(workerID)
	}

	wg.Wait()

	for _, vote := range votes {
		if !vote {
			fmt.Println("block rejected")
			return
		}
	}
	fmt.Println("block accepted")
}
```

What happens in the example:

1. Transactions are split among three goroutines. Partition preparation may finish at different times.
2. `Before` establishes the start boundary: a fast worker cannot validate its partition while the other two are still preparing theirs.
3. Each worker writes only to its own `partitionValid` element, so two goroutines never write the same cell concurrently.
4. `After` establishes the completion boundary. Once it returns, every element of `partitionValid` has been written and will no longer change.
5. Each worker sees the result for the whole block and produces a vote. A real application could build an execution result, sign a vote, or move to the next block height here.

This example synchronizes stages **inside one node**. Coordinating blockchain nodes running in separate processes or on separate machines requires a network protocol, timeouts, and consensus rules. An in-memory barrier does not solve that problem.

### Operations

| Function | Signature | What it does |
|---|---|---|
| `NewBarrier` | `func NewBarrier(size int) *Barrier` | creates a barrier for a fixed number of participants |
| `Before` | `func (b *Barrier) Before()` | waits until every participant has entered the current cycle |
| `After` | `func (b *Barrier) After()` | waits until every participant has finished the parallel part of the cycle |

### When to use it

A barrier fits when a business operation consists of repeated phases and the next phase must not begin until every worker has completed the previous one. Examples include:

- calculating risk for several portfolios before producing a combined report;
- processing image segments in parallel before assembling a frame;
- running a step-based simulation where the next tick reads state produced by every participant during the previous tick;
- checking block partitions before calculating a combined result or producing a vote;
- loading a batch of data before all workers move to processing at the same time.

The business invariant matters more than the mechanism: the current phase's data must be complete before anyone starts the next phase. If the operation has no such boundary, a barrier only adds waiting.

Do not use this `Barrier` when the number of participants may change, an operation must be cancellable, a worker may exit early, or an external dependency may hang. Those cases need a protocol with `context.Context`, a timeout, and explicit failure handling. Use `sync.WaitGroup` for a one-time wait for a set of goroutines, `sync.Mutex` to protect shared data, and a semaphore or worker pool to limit concurrent work.

### Usage rules

- Pass the number of goroutines that will participate in **every** cycle to `NewBarrier`.
- Call the methods in the same order in every goroutine: `Before`, the work itself, then `After`.
- Do not `return` between `Before` and `After`. Store the error as a result, call `After`, and decide whether to stop after the barrier.
- Do not start a new cycle after skipping the previous cycle's `After`.
- Do not call `After` without its corresponding `Before`.
- Run concurrency tests with `go test -race ./...`.
