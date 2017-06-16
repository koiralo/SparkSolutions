import com.amita.{MyEmptyList, MyNonEmptyList}


val data = List(List("Hello ", "how ", "are ", "you "), List("fine ", "thank ", "you"))
data.reduce(_ ::: _)

/*val map1 = Map("number"->"7","name"->"Jane","city"->"New York")
val map2 = Map(1->1, 2->2)

map1.zip(map2)


map1.zipAll(map2, "for missing ", (1000-> 1000))

val x = "shankar"

def f1(op: Option[String]) = op.map(x => Seq(x))

def f2(op: Option[String]) = op.map(Seq(_))

case class Person(name: String)
def f3(op: Option[Person]) = op.map(x => Seq(x))

def f4(op: Option[Person]) = op.map(Seq(_))*/
val list = MyNonEmptyList(1,MyEmptyList)
val b = list.head