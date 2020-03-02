# Expression Documentation
## !=
### Calling


* !=(left, right)

### Arguments



### Description
Not equal.

---

## *
### Calling


* *(left, right)

### Arguments


* must satisfy one of 

	* index 1 family must be same
	* index 2 family must be same
 

### Description
Returns the dot product of the two arguments.

---

## +
### Calling


* +(left, right)

### Arguments


* must satisfy one of 

	* index 1 family must be same
	* index 2 family must be same
 

### Description
Returns the sum of the two arguments.

---

## -
### Calling


* -(left, right)

### Arguments


* must satisfy one of 

	* index 1 family must be same
	* index 2 family must be same
 

### Description
Returns the difference between the two arguments.

---

## /
### Calling


* /(left, right)

### Arguments


* must satisfy one of 

	* index 1 family must be same
	* index 2 family must be same
 

### Description
Returns the division of the two arguments.

---

## <
### Calling


* <(left, right)

### Arguments



### Description
Less than.

---

## <=
### Calling


* <=(left, right)

### Arguments



### Description
Less than or equal to.

---

## !=
### Calling


* !=(left, right)

### Arguments



### Description
Not equal.

---

## =
### Calling


* =(left, right)

### Arguments



### Description
Equal.

---

## >
### Calling


* >(left, right)

### Arguments



### Description
Greater than.

---

## >=
### Calling


* >=(left, right)

### Arguments



### Description
Greater than or equal to.

---

## AND
### Calling


* AND(left, right)

### Arguments



### Description
Logic AND.

---

## COUNT
### Calling



### Arguments



### Description
Averages elements in the group.

---

## IF
### Calling



### Arguments


* exactly 3 arguments must be provided
* the 1st argument must be of type Bool  
* index [1 2] type must be same

### Description
IF (<cond>, <expr1>, <expr2>). Evaluates <cond>, then evaluates <expr1> if the condition is true, or <expr2> otherwise.

---

## LIKE
### Calling


* LIKE(left, right)

### Arguments



### Description
LIKE.

---

## LOGMOCK
### Calling



### Arguments



### Description
Returns a mock log table.

---

## MAX
### Calling



### Arguments


* must satisfy one of 

	* index 1 family must be same
	* index 2 family must be same
 

### Description
Takes the maximum element in the group. Works with Ints, Floats, Strings, Booleans, Times, Durations.

---

## MIN
### Calling



### Arguments


* must satisfy one of 

	* index 1 family must be same
	* index 2 family must be same
 

### Description
Takes the minimum element in the group. Works with Ints, Floats, Strings, Booleans, Times, Durations.

---

## NOT LIKE
### Calling


* NOT LIKE(left, right)

### Arguments



### Description
NOT LIKE.

---

## OR
### Calling


* OR(left, right)

### Arguments



### Description
Logic OR.

---

## RANDTABLE
### Calling



### Arguments


* at least 3 arguments may be provided
* the 1st argument must be of type Int32  
* the 2nd argument must be of type Int32  

### Description
Returns a random list of tuples.

---

## RANGETABLE
### Calling



### Arguments


* at least 3 arguments may be provided
* the 1st argument must be of type Int32  
* the 2nd argument must be of type Int32  

### Description
Returns a list of tuples.

---

## SUM
### Calling



### Arguments


* must satisfy one of 

	* index 1 family must be same
	* index 2 family must be same
 

### Description
Sums Floats, Ints or Durations in the group. You may not mix types.

---

## ZIP
### Calling



### Arguments


* at least 2 arguments may be provided
* must satisfy one of 

	* all arguments must be of type Tuple  
 

### Description
Returns a zip list of tuples.