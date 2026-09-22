# Transformer

<p style="text-align: left">
  <a href="#русский">Русский</a> ・ <a href="#english">English</a>
</p>

---

## Русский

Transformer — стадия конвейера, которая читает значения из канала, применяет к каждому одну функцию и передаёт результат в новый канал. Для одного входного элемента создаётся ровно один выходной элемент. Порядок сохраняется: результат для второго значения не появится раньше результата для первого.

Реализация выполняет `action` последовательно в одной горутине. Она не ускоряет тяжёлое вычисление сама по себе; её задача — вынести преобразование в отдельную потоковую стадию и связать её с другими стадиями через каналы.

### Как это устроено

`Transform[T any](inputCh <-chan T, action func(T) T) <-chan T` создаёт небуферизованный `outputCh` и запускает горутину. Горутина читает `inputCh` через `range`, вызывает `action` для каждого значения и отправляет возвращённое значение в `outputCh`.

После закрытия `inputCh` цикл завершается, а `defer close(outputCh)` закрывает выход. Потребитель может читать результат обычным `range` и не должен закрывать выходной канал самостоятельно.

Небуферизованный `outputCh` создаёт обратную нагрузку. Transformer может получить значение и выполнить `action`, но не возьмёт следующий элемент, пока потребитель не примет текущий результат. Поэтому медленный потребитель постепенно останавливает всю цепочку вплоть до производителя входных данных.

Тип до и после преобразования совпадает: функция имеет форму `T -> T`. Можно вычислить новые поля структуры, нормализовать строку или изменить числовое значение, но нельзя напрямую превратить `Transaction` в `Receipt`. Для преобразования между разными типами потребуется вариант с двумя параметрами типов: `Transform[T, R any](<-chan T, func(T) R) <-chan R`.

### Как вызывать

В примере узел получает неподтверждённые транзакции, нормализует хеш и вычисляет полную сумму списания с учётом комиссии. Следующая стадия уже не повторяет эту бизнес-логику и работает с подготовленными полями.

```go
type Transaction struct {
	Hash       string
	AmountSats int64
	FeeSats    int64
	DebitSats  int64
}

func ExampleTransform() {
	pending := make(chan Transaction, 2)
	pending <- Transaction{
		Hash:       "0xABC",
		AmountSats: 100,
		FeeSats:    2,
	}
	pending <- Transaction{
		Hash:       "0xDEF",
		AmountSats: 250,
		FeeSats:    5,
	}
	close(pending)

	prepare := func(tx Transaction) Transaction {
		tx.Hash = strings.ToLower(tx.Hash)
		tx.DebitSats = tx.AmountSats + tx.FeeSats
		return tx
	}

	for tx := range Transform(pending, prepare) {
		fmt.Printf("tx=%s debit=%d sats\n", tx.Hash, tx.DebitSats)
	}
}
```

Результат:

```text
tx=0xabc debit=102 sats
tx=0xdef debit=255 sats
```

Что происходит в примере:

1. `pending` содержит две транзакции в том порядке, в котором их принял узел.
2. Transformer берёт первую транзакцию и вызывает `prepare`.
3. `prepare` приводит хеш к единому регистру и один раз рассчитывает `DebitSats` по правилу `amount + fee`.
4. Подготовленная транзакция отправляется потребителю. Только после её передачи Transformer переходит к следующей.
5. После закрытия `pending` и обработки оставшихся значений выходной канал закрывается автоматически.

Порядок здесь может быть частью бизнес-контракта: журнал индексатора или локальная очередь узла увидит транзакции в той же последовательности, что и входной поток. При этом Transform не проверяет подпись, баланс отправителя или допустимость комиссии — такие проверки должны быть отдельными явно названными стадиями.

### Операция

| Функция | Сигнатура | Что делает |
|---|---|---|
| `Transform` | `func Transform[T any](inputCh <-chan T, action func(T) T) <-chan T` | последовательно применяет `action` ко всем входным значениям и закрывает выход после завершения входа |

### Когда использовать

Transformer подходит для потокового правила «один элемент на входе — один элемент на выходе», когда порядок важен, а всё преобразование помещается в обычную функцию. Например:

- нормализация идентификаторов, адресов или текстовых полей;
- вычисление производных полей транзакции;
- добавление локальных метаданных к событию;
- приведение денежных значений к одной единице измерения;
- построение последовательного конвейера из нескольких небольших стадий.

Стадии можно соединять напрямую:

```go
normalized := Transform(inputCh, normalize)
enriched := Transform(normalized, calculateFee)
```

Такое разделение удобно, если у стадий разные обязанности и их нужно тестировать независимо. Однако каждый вызов `Transform` создаёт отдельную горутину и канал. Для двух дешёвых операций один общий `action` обычно проще и дешевле.

Не используйте эту реализацию для параллельной CPU-нагрузки: `action` вызывается только одной горутиной. Для независимой обработки большого потока нужен worker pool с последующим восстановлением порядка, если он важен. Для удаления части элементов нужен filter, для объединения нескольких источников — fan-in или bridge, для преобразования `T` в другой тип — обобщённая функция с отдельным типом результата.

### Ограничения и ошибки

У `action` нет результата `error`. Если преобразование может штатно завершиться ошибкой, не следует скрывать её нулевым значением. Добавьте статус ошибки в тип результата или измените API так, чтобы выход содержал `Result[T]` с полями значения и ошибки.

Паника внутри `action` происходит в служебной горутине и без восстановления завершит программу. `nil` вместо `action` также вызовет панику на первом входном значении. Передаваемая функция должна быть определена и не паниковать на допустимых данных.

Значение `T` передаётся по значению, но это не означает глубокую копию. Если структура содержит slice, map или pointer, `action` может изменить память, общую с производителем или другими стадиями. Владение такими данными должно быть определено отдельно либо данные нужно копировать явно.

Текущий API не поддерживает `context.Context`. Если `inputCh` не закрывается или потребитель перестаёт читать `outputCh`, служебная горутина может остаться заблокированной.

### Правила использования

- Закрывайте `inputCh`, когда новых значений больше не будет.
- Всегда вычитывайте `outputCh` либо предусматривайте отмену всего конвейера.
- Делайте `action` детерминированной и быстрой, если важна предсказуемая пропускная способность.
- Не изменяйте через `action` разделяемые map, slice или указатели без явной синхронизации.
- Не закрывайте канал, возвращённый `Transform`: его закрывает сама стадия.
- Для ошибок используйте явный тип результата, а не специальные или нулевые значения.
- Проверяйте конкурентный код командой `go test -race ./...`.

---

## English

Transformer is a pipeline stage that reads values from a channel, applies one function to each of them, and sends the result to a new channel. Every input produces exactly one output. Order is preserved: the result for the second value cannot appear before the result for the first.

This implementation executes `action` sequentially in one goroutine. It does not make expensive computation faster by itself; its purpose is to isolate a transformation as a streaming stage and connect it to other stages through channels.

### How it works

`Transform[T any](inputCh <-chan T, action func(T) T) <-chan T` creates an unbuffered `outputCh` and starts a goroutine. That goroutine ranges over `inputCh`, invokes `action` for each value, and sends the returned value to `outputCh`.

Once `inputCh` closes, the loop ends and `defer close(outputCh)` closes the output. The consumer can range over the result and must not close the output channel itself.

The unbuffered `outputCh` provides backpressure. Transformer may receive a value and execute `action`, but it cannot take the next item until the consumer accepts the current result. A slow consumer therefore slows the whole chain, eventually reaching the input producer.

The input and output types are identical: the function has the shape `T -> T`. It can calculate struct fields, normalize a string, or change a numeric value, but it cannot directly turn a `Transaction` into a `Receipt`. Mapping between different types requires a version with two type parameters: `Transform[T, R any](<-chan T, func(T) R) <-chan R`.

### How to call it

In this example, a node receives pending transactions, normalizes each hash, and calculates the total debit including the fee. The next stage can use the prepared fields without duplicating this business rule.

```go
type Transaction struct {
	Hash       string
	AmountSats int64
	FeeSats    int64
	DebitSats  int64
}

func ExampleTransform() {
	pending := make(chan Transaction, 2)
	pending <- Transaction{
		Hash:       "0xABC",
		AmountSats: 100,
		FeeSats:    2,
	}
	pending <- Transaction{
		Hash:       "0xDEF",
		AmountSats: 250,
		FeeSats:    5,
	}
	close(pending)

	prepare := func(tx Transaction) Transaction {
		tx.Hash = strings.ToLower(tx.Hash)
		tx.DebitSats = tx.AmountSats + tx.FeeSats
		return tx
	}

	for tx := range Transform(pending, prepare) {
		fmt.Printf("tx=%s debit=%d sats\n", tx.Hash, tx.DebitSats)
	}
}
```

Output:

```text
tx=0xabc debit=102 sats
tx=0xdef debit=255 sats
```

What happens in the example:

1. `pending` contains two transactions in the order accepted by the node.
2. Transformer receives the first transaction and calls `prepare`.
3. `prepare` normalizes the hash and calculates `DebitSats` once using the `amount + fee` rule.
4. The prepared transaction is sent to the consumer. Transformer moves to the next one only after that send completes.
5. Once `pending` is closed and all remaining values are processed, the output closes automatically.

Order can be part of the business contract here: an indexer's journal or a node's local queue observes transactions in the same sequence as the input stream. Transform does not verify signatures, sender balances, or fee policy; those checks should be separate, explicitly named stages.

### Operation

| Function | Signature | What it does |
|---|---|---|
| `Transform` | `func Transform[T any](inputCh <-chan T, action func(T) T) <-chan T` | applies `action` to every input sequentially and closes the output after the input ends |

### When to use it

Transformer fits a streaming “one item in, one item out” rule when order matters and the whole conversion can be expressed as a regular function. Examples include:

- normalizing identifiers, addresses, or text fields;
- calculating derived transaction fields;
- adding local metadata to an event;
- converting monetary values to one unit;
- building a sequential pipeline from several small stages.

Stages can be connected directly:

```go
normalized := Transform(inputCh, normalize)
enriched := Transform(normalized, calculateFee)
```

This separation is useful when stages have distinct responsibilities and need independent tests. Each `Transform` call, however, adds a goroutine and a channel. Combining two cheap operations into one `action` is usually simpler and less expensive.

Do not use this implementation for parallel CPU work: only one goroutine invokes `action`. Independent processing of a large stream calls for a worker pool, followed by an ordering step if order matters. Use a filter to remove items, fan-in or bridge to combine sources, and a generic function with a separate result type to map `T` into another type.

### Limitations and errors

`action` has no `error` result. If transformation can fail as part of normal operation, do not hide failure behind a zero or sentinel value. Add error state to the output type, or change the API so the output carries a `Result[T]` containing both a value and an error.

A panic inside `action` occurs in the worker goroutine and terminates the program unless recovered. Passing a `nil` action also panics on the first input value. The supplied function must be defined and safe for valid input.

`T` is passed by value, but this is not a deep copy. If a struct contains a slice, map, or pointer, `action` can mutate memory shared with the producer or another stage. Establish ownership of such data or copy it explicitly.

The current API has no `context.Context`. If `inputCh` never closes or the consumer stops reading `outputCh`, the worker goroutine may remain blocked.

### Usage rules

- Close `inputCh` when no more values will arrive.
- Always drain `outputCh`, or provide cancellation for the whole pipeline.
- Keep `action` deterministic and fast when predictable throughput matters.
- Do not use `action` to mutate shared maps, slices, or pointed-to values without explicit synchronization.
- Do not close the channel returned by `Transform`; the stage owns it.
- Represent failures with an explicit result type, not zero or sentinel values.
- Check concurrent code with `go test -race ./...`.
