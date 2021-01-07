# cat.py
import float_1
from float_1 import Animal


class Cat(Animal):
    x = 'Cat'
    y = 'abcd'


class Dog(Animal):
    def __dir__(self):
        return ['dog']  # 必须返回列表


print('-------------')
print("Current Module\s'names = {}".format(dir())) # 模块名词空间内的属性
print('animal Module\'s names = {}'.format(dir(float_1)))  # 指定模块名词空间内的属性
print("object's __dict__  = {}".format(sorted(object.__dict__.keys())))  # object的字典
print("Animal's dir() = {}".format(dir(Animal)))  # 类Animal的dir()
print("Cat's dir() = {}".format(dir(Cat)))  # 类Cat的dir()
print('~~~~~~~~~~~~~~')
tom = Cat('tome')
print(sorted(dir(tom)))  # 实例tom的属性、Cat类及所有祖先类的类属性
print(sorted(tom.__dir__()))  # 同上
# dir() 的等价 近似如下, __dict__ 字典中几乎包括了所有属性
print(sorted(set(tom.__dict__.keys()) | set(Cat.__dict__.keys()) | set(object.__dict__.keys())))

print("Dog's dir = {}".format(dir(Dog)))
dog = Dog('snoppy')
print(dir(dog))
print(dog.__dict__)