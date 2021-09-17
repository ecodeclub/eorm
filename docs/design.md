# 设计思路

`EQL`从某种程度上来说，只是我关于`ORM`计划的一个起点。我在维护 `Beego` 的`ORM`的时候，以及在使用`GORM`，`Java`的`Mybatis`, `Hibernate`的时候，意识到这样一个问题：`ORM`框架，严格来说，应该是可以分成三个部分的：

- 查询语言：也就是`EQL`这块，它主要是负责生成`SQL`，以及运行`SQL`的参数；
- 结果集处理：也就是将`SQL`查询结果转化为结构体，或者集合，该部分的典型例子是`sqlx`，这个部分初步的名字是`EMP`；
- 结合层：将查询语言和结果集处理两个粘在一起，对外表现出来的样子，就是`ORM`。如果考虑得更加复杂一点，那么数据源管理，比如说测试库切换，以及分库分表，都可以尝试在这一层上来完成；

其中查询语言和结果集处理，非常独立，以至于完全可以独立出来作为一个库，也可以被单独使用。在我的设想中：

1. 用户可以尝试结合`EQL`和`sqlx`，利用`EQL`来生成`SQL`，利用`sqlx`来将结果集组装为对象；
2. 用户可以尝试使用`EQL`与其它`ORM`，利用`EQL`来生成`SQL`，调用其它`ORM`的`RawSQL`方法；
3. 用户可以自己手写`SQL`，然后自己用`sql`库运行拿到结果集，然后丢到`EMP`，`EMP`来完成对象组装；
4. 用户可以利用`EMP`或`EQL`中的任何一个部分，或者两个部分，设计自己的`ORM`框架。也就是意味着有了`EQL`和`EMP`，设计`ORM`框架的事情，应该是一个稍微具有经验的开发就能够胜任的工作。任何一个`ORM`框架都无法满足所有人对`ORM`的期望，所以与其求人，不如求己，利用`EQL`和`EMP`自己设计自己需要的`ORM`；

因此，第一步，就是设计一个`EQL`，即查询语言。

## EQL 设计要点

一个查询语言的设计，第一点是要做到**完备性**。即我们的目标是能够支持全部的`SQL`语法特性。但是实际上，这是一个近乎不可能的任务，即便只考虑`MySQL`也是一个几乎不可能的问题。更何况，`EQL`的目标是能够兼容卷大多数的方言。

即便我只寻求尽可能完备，但是对于设计`API`来说也是一个巨大的挑战。比如说，这是`MySQL`上的`SELECT`语句的规范：

![MySQL SELECT](2021-09-17-00-00-28.png)

鉴于此，我肯定没办法在第一个版本的时候就支持所有的特性，因此，设计`API`的第二个点，是要**易扩展**。这个易扩展不是指我一个接口容易写好几个实现的易扩展，而是容易不断加方法来支持新的语法特性。

还要考虑的一点是，`API`应该是**符合直觉的**。也就是，即便用户在不看文档的情况，仅仅是通过`IDE`的提示，他也应该能直接学会如何使用。这要求的是，有什么方法，方法名字叫啥，以及参数是啥，都应该是符合大多数人的淳朴的认知的。

最后一点，则是要保证一种向后**兼容性**。用户肯定不会希望，升级了一个版本之后，原来的方法就不再能够使用了。向后兼容性这个要求，迫使我们在设计每一个方法的时候，都要谨慎行事，也迫使在实现的时候，尽量不暴露细节。

我早期看过很多的`ORM`的查询语言的设计，大体上，在`GO`这边，模式都是类似的：

```go
type Orm interface {
    Create(val...interface)
    Update() 
    Delete()
    Query() *QueryBuilder
}
```
首先是几乎都有一个大一统的`ORM`接口。该接口提供了主要的增删改查方法，有一些还额外提供了类似于`QueryBuilder`的接口，用于构造复杂的查询。显然，前者，大一统的`ORM`接口是完全无法满足**完备性**和**扩展性**。

## Builder 模式

因此我一直在寻求一种更加优雅的设计。

后来在我司，接触了一个新的`ORM`之后，得到了极大的启发：`SQL`的构建，是`Builder`模式的最佳使用场景。正如我前面提到，其实别的框架里面也有类似`Builder`的东西，比如说`Beego`里面有一个`QueryBuilder`。不过一直以来它都是作为一个辅助接口，核心接口是`Orm`接口，导致我一直对它重视不够，所谓灯下黑也不过如此。另外一个原因是我早期尝试设计`Builder`的时候，走上了歧途：

```go
type Builder interface {
    Build() Query
    // 一些构造公共子部分的方法
    BuildWhere()
}
type SelectBuilder interface {
    // 一些用于构造 Select 的方法
}
```

在这种设计之下，不同的方法会有不同的实现，因此可以保证做到方便横向扩展。

但是使用这种设计也意味着，我每次需要增加一个语法特性，就要在上面增加一个方法。还有一个非常非常难以解决的问题：方言语法特性不兼容。比如说，在`PGSQL`里面，`upsert`的语法是`ON CONCLICT`，而`MySQL`的语法是`OnDuplicateKey`。这两者之间的语法差异还是挺大，因此难以在`SelectBuilder`里面加上一个大一统的这种触发`upsert`的方法。就最简单的命名来说，使用`OnConflict`导致`MySQL`用户看起来这个方法不够直观，使用`OnDuplicateKey`，`PGSQL`看起来不够直观。不够直观就意味着他需要找文档，看例子才能学会如何使用。

在我司得到启发之后，我突然就想通了两个问题：
- 我实际上只需要一个`Builder`接口，该接口就是返回`SQL`和参数，以及`error`。`error`用于判断是否构造成功；
- 具体的如何构造，我完全不需要在`Builder`里面定义方法，就各自实现类里面去定义方法

所以，`EQL`就有了最顶级的接口设计：

```go
// QueryBuilder is used to build a query
type QueryBuilder interface {
	Build() (*Query, error)
}

// Query represents a query
type Query struct {
	SQL string
	Args []interface{}
}
```

该接口是最最核心的接口，剩余的一切结构体和接口，都是为这个服务的。

目前基于该接口直接有四个实现：
- `SELECTOR`
- `UPDATER`
- `DELETER`
- `INSERTER`

### 方法命名、参数以及构造SQL

以设计`SELECTOR`为例，展示一下方法命名、参数和一些构造`SQL`上的注意点。

首先第一条原则是：**遵循`SQL`的命名**。也就是说，`SQL`里面叫啥，`EQL`里面的方法就叫啥。这是`EQL`在`API`设计上显著不同于别的`ORM`框架的点，也显著不同于我司的`ORM`。

> 从一个`ORM`的角度来说，这条原则是错的。因为`ORM`在用户看来，方法命名应该遵循面向对象的那一套。比如说，`INSERTER`应该叫做`CREATOR`会更加符合面向对象的感觉

所以，我期望的用户用`EQL`就像写一条`SQL`：

```go
Select("Id", "Name").From(&TestModel{}).Where(predicates).OrderBy("Id DESC").Offset(10).Limit(100).Build()
```
在参数传递上，传递的是**字段名**，而不是列名。这是第二条原则：**用户在操作所有的`API`的时候，都应该传递的是`GO`（应用层面）层面上的信息，而不是数据库层面上的信息**。例如`Name`是`Go`中结构体字段的名字，与之对应的列名应该是`name`。在这种场景下，`EQL`将完成`Go`语言层面到数据库层面的映射。

最终，用户通过调用`Build`方法来拿到`SQL`和`SQL`的参数。然而，我期望用户按照这样一种很自然的，和自己写`SQL`差不多的顺序来调用方法，最终完成`SQL`的构建。但是其实用户是不一定会按照期望的顺序来的。比如说他可能把`OrderBy`放在`Where`之前，在这种时候，我还是期望用户能够得到正确的结果。因为在有些时候，用户自己也不能控制住顺序，特别是在一些根据条件来构造的情况。

所以，`Build`方法就承担了最重要的职责，也就是我们的第三条原则：**`SQL`的构造仅能在`Build`方法中完成**。由此又可以将这些`Builder`的方法分成两类：
- 终结方法：`Build`；
- 中间方法：其它方法。这些方法的职责就是保存一份配置，比如说用户调用了`Where`，那么应该把`Where`的输入保存下来，最终在`Build`方法里面把`Where`构造出来；

这种机制，还额外带来了两个收益：

- 将内存分配集中在`Build`方法中：意味着，可以使用一个`strings.Builder`来完成整个`SQL`构建。同时也可以在将来考虑内存复用——比如说使用`sync.Pool`来缓存`buffer`；
- 快速失败：在`Build`中构造`SQL`，那么任何一个部分失败之后，后面的部分就不需要再执行下去了。而其它方法因为非常轻量，所以开销非常小；

另外还有一个约束：**所有的`API`，如果参数是模型，那么应该只接收指针**。这应该作为一条一致性的契约，贯穿所有的公共`API`设计。这样带来两方面的好处，一者是用户不必考虑是否应该传入指针；另外一个是实现的时候也不必次次都检查和转换。这属于和用户的一种默契与约定。

## DB 抽象

即便`EQL`并不需要真的执行`SQL`，但是`EQL`依旧需要一个`DB`的抽象，它主要承担的作用就是维护一些“配置”，或者说和`DB`相关的元数据，例如说方言。
```go
type DB interface {
    dialect dialect
    registry MetaRegistry
}

type dialect struct {

}
```

同时还要维持所有的表元数据，表元数据可以看**元数据**一章。

## 元数据

在构造`EQL`的时候，需要知道一些表的元数据，比如说表名、字段名。它们通常是通过解析结构体来得到的。

因此需要考虑两个维度的元数据，表层次上的元数据和列层次上的元数据。再进一步考虑到`EQL`的定位——可以与别的`ORM`框架（包括EMP）结合，那么显然要存在一种机制，允许用户自定义如何解析元数据。

因此元数据的解决方案，要涉及两方面：元数据定义本身和元数据提供者。对应到`EQL`，就是三个接口：

```go
type TableMeta struct {
}

type ColumnMeta struct {

}

type MetaRegistry interface {
	Get(table interface{}) (*TableMeta, error)
	Register(table interface{}) error
}
```

`EQL`本身提供一种默认实现，该默认实现是基于反射和`Tag`，从结构体中解析出来。例如说以`eql`作为`Tag`，那么结构体定义形如：
```go
type User struct {
    Email string `eql:"unque_index",
}
```

### 默认解析

默认解析应该遵循的原则：
1. 表名字段名应该遵循：驼峰转下划线。例如`OrderDetail`对应的表名是`order_detail`，而字段`FirstName`对应得是`first_name`；

### 自定义元数据

有些时候需要提供一些接口来允许用户自定义一些信息，比如说表名，字段名到列名的映射。用于解决这么一类典型场景：
1. 用户拿到的结构体是第三方的结构体，因而用户无法在字段上加上`eql`的`Tag`。这种需求在使用`IDL`的公司较为常见。例如`protobuf`，`swagger`等；
2. 用户不满意于默认规则，例如按照约定`User`这个结构体对应的表名是`user`，而`user`在数据库里面有自己的含义，因此用户希望换成`user_tab`。大多数时候，也可能是因为公司有命名规范，比如说所有的表名，起始都应该是`t_`；

要同时满足着两个场景，需要考虑引入注册的过程：

```go
type MetaOption func(meta *TableMeta)

func WithTableName(tn string) MetaOption {
	return func(meta *TableMeta) {
		meta.Name = tn
	}
}

registry.Register(&User{}, opts)
```
`opts`就是用户传入的各种自定义的修改方法。

### MetaRegistry 实现要点

#### 缓存

元数据应当是可以被缓存的。一个表结构在系统运行期间是稳定的。毕竟如果要增加字段或者修改字段，无法避免的需要修改代码，因而自然就会重启。所以元数据缓存之后是可以完全不考虑淘汰的问题的。

但是另外一个问题是，如果元数据是可以缓存的，那么我是否应该在应用启动之前，强制要求用户注册所有的模型？

```go
registry.Init(&User)
app.Run() //...
```
这个问题的答案是：取决于`MetaRegistry`的实现。即我认为，用户出于性能考虑可以自己提供一种提前注册的实现。因为“是否缓存以及如何预热缓存”，完全是属于`MetaRegistry`的实现细节。

在`EQL`的默认实现里，因为要解决**自定义元数据**的问题，所以必然是要提供注册的方法，而且为了简化操作，应该附加在`DB`上，这样一来，还可以强化用户将`DB`作为全局唯一实例来使用：

```go
func (d *DB) RegisterTable(table interface) error {
    d.registry.Register(table)
}
```

#### 维度

在考虑应用缓存的时候，就要考虑另外一个问题：`MetaRegistry`的维度问题。

理想情况下，因为元数据是和表相关，而表是归属于数据库的，所以理论上来说，`MetaRegistry`和`DB`应该是一比一的关系，即一个`DB`有一个`Factory`。但是在当前的设计下，无法限制用户对于一个`DB`只创建一个实例，他可能在局部变量里面创建`DB`。

例如我期望的是：
```go

userDB := eql.New(opts)

func Biz1() {
    userDB.Select() //...
}

func Biz2() {
    userDB.Select() //...
}
```
而用户可能用法是：
```go
func Biz1() {
    eql.New(opts).Select() //...
}

func Biz2() {
    eql.New(opts).Select() //...
}
```
如果采用全局唯一实例，那么就要考虑模型冲突的问题。即在多数据库系统中，可能在多个数据库中都有同样的叫做`User`的表，但是在结构上有细微的不同。


## 方言兼容方案

方言兼容方案，主要是两个角度：
- 方言独有语法特性；
- 所有（或者大多数）都有的特性，但是实现上有细微差别；

对于第一个点来说，很好办，直接衍生出来一个新的`Builder`，这个`Builder`是针对特定方言的。用`upsert`来举例子，`Inserter`里面，提供了两个方法来返回不同方言的`Upserter`：

```go
// OnDuplicateKey generate MysqlUpserter
// if the dialect is not MySQL, it will panic
func (i *Inserter) OnDuplicateKey() *MysqlUpserter {
	panic("implement me")
}

// OnConflict generate PgSQLUpserter
// if the dialect is not PgSQL, it will panic
func (i *Inserter) OnConflict(cs ...string) *PgSQLUpserter {
	panic("implement me")
}
```
注意的是`Inserter`，对于大多数方言都是一样的，只有在过渡到`upsert`语句的时候，开始分叉。例如在`PgSQL`里面，标记着`upsert`是`ON CONFLICT`，那么使用`PgSQL`的人，在使用`Inserter`的时候，会下意识地选择`OnConflict`。同样的道理，`MySQL`的用户天然就知道自己应该使用`OnDuplicateKey`。

> 当然，这也会给一些用户，同时知晓`MySQL`和`PgSQL`，或者很聪明的人，带来一些困惑，就是我该用哪个，但是只要看到返回值，或者方法注释，应该也是很容易就理解的。

针对第二点，最典型的例子是引号，比如说字符串的引号，字段名的引号，不同方言都有自己的特点。这一类，通过引入`dialect`的抽象来解决：

```go
type dialect struct {
    quote byte // MySQL => `
}
```

## 启用泛型

这一次，因为恰好`GO`的泛型方案也快要出来了，所以这一次会直接启用`Go1.17`之后的版本，在有必要的时候启用泛型。所谓有必要，是指在`EQL`这里暂时还看不到必要性。不过目测在`EMP`里面就要启用泛型了。

另外现在不立刻启用泛型，也是因为`IDE`对泛型的支持还不是很好，代码写起来特别累。