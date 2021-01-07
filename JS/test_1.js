
// 类型转换
// 弱类型
console.log('========string=======')
console.log(a = 3 + 'mongodb',  typeof(a))
console.log(a = null  +  'mongodb',  typeof(a))
console.log(a = undefined + 'mongodb',  typeof(a))
console.log(a = true + 'mongodb',  typeof(a))



// 数字
console.log('========number=========')
console.log(a = null + 8, typeof(a))
console.log(a = undefined + 8, typeof(a))
console.log(a = true + 8, typeof(a)) // 1
console.log(a = false + 8, typeof(a))

// boolean
console.log('============bool=========')
console.log(a = null + true, typeof(a))
console.log(a = null + false, typeof(a))
console.log(a = undefined + true, typeof(a)) // undefined设法转换成一个对应的数字
console.log(a = undefined + false, typeof(a)) // 1
console.log(a = null & true, typeof(a))
console.log(a = undefined & true, typeof(a))


// // 短路
console.log('~~~~~~~~~~~~~~~~~~~~~~~~~')
console.log(a = null && true, typeof(a)) // 逻辑运算符, null 直接就是false短路
console.log(a = false && null , typeof(a)) // 逻辑运算符, false 短路返回false
console.log(a = false && 'mongodb', typeof(a)) // boolean
console.log(a = true && 'mongodb', typeof(a)) // 字符串
console.log(a = true && '', typeof(a)) // 字符串

// // null
// console.log('============null========')
// console.log(a = null + undefined, typeof(a))


