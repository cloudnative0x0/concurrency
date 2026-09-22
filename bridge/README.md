# Bridge

<p style="text-align: left">
  <a href="#русский">Русский</a> ・ <a href="#english">English</a>
</p>

---

## Русский

Bridge превращает последовательность каналов в один канал значений. На вход приходит `chan chan T`: каждый элемент внешнего канала — отдельный поток данных. На выходе получается обычный `<-chan T`, из которого вызывающий код читает все значения без ручного переключения между внутренними каналами.

Важное свойство этой реализации — строгий порядок. Bridge полностью вычитывает первый внутренний канал, ждёт его закрытия и только потом берёт следующий. Это не конкурентный fan-in: медленный или незакрытый источник блокирует доступ ко всем источникам, которые стоят после него.

### Как это устроено

`Bridge[T any](inputChCh chan chan T) <-chan T` создаёт `outputCh` и запускает одну горутину. Внешний цикл читает каналы из `inputChCh`, внутренний цикл читает значения из текущего `inputCh` и по одному пересылает их в `outputCh`.

Когда внутренний канал закрывается, `range` переходит к следующему каналу. Когда закрывается внешний `inputChCh` и все уже полученные внутренние каналы вычитаны, горутина завершает работу и закрывает `outputCh` через `defer`.

Выходной канал не имеет буфера. Если потребитель перестал читать, bridge останавливается на записи в `outputCh`. Вслед за ним может остановиться производитель текущего внутреннего канала. Такая обратная нагрузка полезна, когда источник не должен обгонять обработчик, но её необходимо учитывать при расчёте пропускной способности.

Bridge не закрывает входные каналы и не может определить, что больше значений не будет. За жизненный цикл отвечают производители:

- производитель внутреннего канала закрывает его после последнего значения;
- производитель внешнего канала закрывает `inputChCh` после последнего канала;
- `nil` вместо внутреннего канала запрещён: чтение из него заблокируется навсегда;
- канал, который никогда не закрывается, навсегда задержит все следующие каналы.

### Как вызывать

В примере индексатор получает финализированные блоки в порядке высоты. Для каждого блока создаётся отдельный канал транзакций. Bridge собирает эти каналы в единый поток, сохраняя порядок блоков и порядок транзакций внутри каждого блока.

```go
type Transaction struct {
	BlockHeight int
	Hash        string
}

func ExampleBridge() {
	blocks := []struct {
		height int
		hashes []string
	}{
		{840_000, []string{"0xa1", "0xa2"}},
		{840_001, []string{"0xb1"}},
		{840_002, []string{"0xc1", "0xc2"}},
	}

	blockStreams := make(chan chan Transaction)
	go func() {
		defer close(blockStreams)

		for _, block := range blocks {
			transactions := make(chan Transaction, len(block.hashes))
			for _, hash := range block.hashes {
				transactions <- Transaction{
					BlockHeight: block.height,
					Hash:        hash,
				}
			}
			close(transactions)
			blockStreams <- transactions
		}
	}()

	for tx := range Bridge(blockStreams) {
		fmt.Printf("apply block=%d tx=%s\n", tx.BlockHeight, tx.Hash)
	}
}
```

Результат:

```text
apply block=840000 tx=0xa1
apply block=840000 tx=0xa2
apply block=840001 tx=0xb1
apply block=840002 tx=0xc1
apply block=840002 tx=0xc2
```

Что здесь происходит:

1. Производитель обходит только финализированную цепочку, поэтому каналы попадают в `blockStreams` в каноническом порядке высот.
2. Канал каждой группы буферизован на число транзакций. Это позволяет заполнить его до отправки во внешний канал без отдельной горутины на блок.
3. Bridge сначала полностью отдаёт транзакции блока `840000`. Лишь после закрытия его канала он переходит к `840001`.
4. Индексатор получает плоский поток и может обновлять балансы, историю адресов или поисковый индекс обычным циклом `range`.
5. Выход закрывается автоматически, когда закончились все блоки. Потребителю не нужно знать, сколько внутренних каналов было создано.

Bridge отвечает только за порядок чтения. Он не проверяет финальность блока, не обрабатывает реорганизации цепочки и не откатывает уже применённые транзакции. Эти правила остаются частью бизнес-логики индексатора.

### Операция

| Функция | Сигнатура | Что делает |
|---|---|---|
| `Bridge` | `func Bridge[T any](inputChCh chan chan T) <-chan T` | последовательно объединяет каналы из `inputChCh` в один выходной канал |

### Когда использовать

Bridge подходит, когда источники появляются динамически, но обрабатывать их нужно строго по очереди. Типичные случаи:

- чтение страниц API, где следующая страница не должна обгонять предыдущую;
- последовательная обработка файлов, каждый из которых читается через отдельный канал;
- применение блоков или журналов событий в порядке высоты либо смещения;
- сбор частей выгрузки, сформированных отдельными этапами;
- преобразование `chan chan T` в интерфейс, удобный для обычного `range`.

Главный бизнес-инвариант — следующий набор нельзя наблюдать до полного завершения предыдущего. Если порядок между источниками не важен и все они должны обслуживаться одновременно, нужен fan-in, а не эта реализация bridge. Если требуется ограничить число параллельных задач, используйте worker pool или семафор.

У паттерна есть head-of-line blocking: один медленный источник задерживает всю очередь. Он особенно опасен, когда внутренний канал зависит от сети или внешнего сервиса. Текущий API не принимает `context.Context`, поэтому отменить зависшее чтение через `Bridge` нельзя. Для таких источников отмену и тайм-аут следует реализовать на стороне производителя либо расширить сам API.

### Правила использования

- Отправляйте внутренние каналы во внешний канал в том порядке, в котором должны появиться их значения.
- Всегда закрывайте каждый внутренний канал после последнего значения.
- Закрывайте `inputChCh`, когда новых каналов больше не будет.
- Не отправляйте `nil`-каналы.
- Не читайте внутренний канал параллельно в другом месте: значения разделятся между двумя потребителями.
- Продолжайте читать `outputCh` либо предусмотрите отмену производителей, иначе цепочка может заблокироваться из-за обратной нагрузки.
- Проверяйте конкурентный код командой `go test -race ./...`.

---

## English

Bridge turns a sequence of channels into one channel of values. Its input is a `chan chan T`, where each value on the outer channel is a separate data stream. Its output is a regular `<-chan T`, so the caller can consume every value without manually switching between inner channels.

This implementation has one defining property: strict ordering. Bridge drains the first inner channel completely, waits for it to close, and only then moves to the next one. It is not a concurrent fan-in: a slow or unclosed source blocks access to every source queued behind it.

### How it works

`Bridge[T any](inputChCh chan chan T) <-chan T` creates `outputCh` and starts one goroutine. The outer loop receives channels from `inputChCh`; the inner loop receives values from the current `inputCh` and forwards them to `outputCh` one at a time.

When an inner channel closes, its `range` ends and the bridge moves to the next channel. When the outer `inputChCh` closes and every channel already received from it has been drained, the goroutine returns and closes `outputCh` through `defer`.

The output channel is unbuffered. If the consumer stops reading, the bridge stops while sending to `outputCh`. The producer of the current inner channel may then stop as well. This backpressure is useful when a source must not outrun its consumer, but it has to be included in throughput and failure analysis.

Bridge does not close its inputs and cannot infer that no more values will arrive. Producers own their channel lifecycle:

- the producer of an inner channel closes it after its final value;
- the producer of the outer channel closes `inputChCh` after its final channel;
- a `nil` inner channel is invalid because receiving from it blocks forever;
- an inner channel that never closes permanently delays every channel after it.

### How to call it

In this example, an indexer receives finalized blocks in height order. Each block gets its own transaction channel. Bridge turns those channels into one stream while preserving both block order and transaction order within each block.

```go
type Transaction struct {
	BlockHeight int
	Hash        string
}

func ExampleBridge() {
	blocks := []struct {
		height int
		hashes []string
	}{
		{840_000, []string{"0xa1", "0xa2"}},
		{840_001, []string{"0xb1"}},
		{840_002, []string{"0xc1", "0xc2"}},
	}

	blockStreams := make(chan chan Transaction)
	go func() {
		defer close(blockStreams)

		for _, block := range blocks {
			transactions := make(chan Transaction, len(block.hashes))
			for _, hash := range block.hashes {
				transactions <- Transaction{
					BlockHeight: block.height,
					Hash:        hash,
				}
			}
			close(transactions)
			blockStreams <- transactions
		}
	}()

	for tx := range Bridge(blockStreams) {
		fmt.Printf("apply block=%d tx=%s\n", tx.BlockHeight, tx.Hash)
	}
}
```

Output:

```text
apply block=840000 tx=0xa1
apply block=840000 tx=0xa2
apply block=840001 tx=0xb1
apply block=840002 tx=0xc1
apply block=840002 tx=0xc2
```

What happens in the example:

1. The producer walks a finalized chain, so channels enter `blockStreams` in canonical height order.
2. Each group channel is buffered for its transaction count. This allows the producer to fill it before sending it to the outer channel, without starting a goroutine per block.
3. Bridge emits every transaction from block `840000` before it starts reading block `840001`.
4. The indexer receives a flat stream and can update balances, address history, or a search index with a regular `range` loop.
5. The output closes automatically after all blocks have been consumed. The consumer does not need to know how many inner channels were created.

Bridge is responsible only for read order. It does not verify block finality, handle chain reorganizations, or roll back transactions that were already applied. Those rules remain part of the indexer's business logic.

### Operation

| Function | Signature | What it does |
|---|---|---|
| `Bridge` | `func Bridge[T any](inputChCh chan chan T) <-chan T` | sequentially flattens the channels received from `inputChCh` into one output channel |

### When to use it

Bridge fits when sources appear dynamically but must be processed in strict sequence. Common cases include:

- reading API pages when a later page must not overtake an earlier one;
- processing files sequentially, with each file exposed through its own channel;
- applying blocks or event logs in height or offset order;
- collecting export chunks produced by separate stages;
- converting a `chan chan T` into an interface that works with a regular `range` loop.

The central business invariant is that the next group must not become visible until the previous group is complete. If ordering between sources does not matter and all of them should be serviced concurrently, use fan-in instead of this bridge implementation. Use a worker pool or semaphore when the goal is to limit concurrent work.

The pattern has head-of-line blocking: one slow source delays the whole queue. This is particularly risky when an inner channel depends on a network or external service. The current API does not accept a `context.Context`, so a stuck read cannot be cancelled through `Bridge`. Implement cancellation and timeouts in the producer, or extend the API for those requirements.

### Usage rules

- Send inner channels to the outer channel in the order in which their values must appear.
- Always close each inner channel after its final value.
- Close `inputChCh` when no more channels will be produced.
- Never send a `nil` channel.
- Do not read an inner channel elsewhere at the same time, or its values will be split between two consumers.
- Keep consuming `outputCh` or provide producer cancellation; otherwise backpressure can block the entire chain.
- Check concurrent code with `go test -race ./...`.
