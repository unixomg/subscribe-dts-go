package utils

import (
	"fmt"
	"goimpl/avro"
	"reflect"
	"time"
	"os"
)

func TimeColumnSplice(year,month,day,hour,minutes,second int32) string  {
	return fmt.Sprintf("%d-%d-%d %d:%d:%d",year,month,day,hour,minutes,second)
}

func DateTimeToStr(datetime *avro.DateTime) string  {
	return fmt.Sprintf("%d-%d-%d %d:%d:%d",datetime.Year.Int,datetime.Month.Int,datetime.Day.Int,datetime.Hour.Int,datetime.Minute.Int,datetime.Second.Int)
}

func TimeStampToStr(tim2stamp int64) string {
	return  time.Unix(tim2stamp,0).Format("2006-01-02 15:04:05")
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}




func Union(slice1, slice2 []string) []string {
	m := make(map[string]int)
	for _, v := range slice1 {
		m[v]++
	}

	for _, v := range slice2 {
		times, _ := m[v]
		if times == 0 {
			slice1 = append(slice1, v)
		}
	}
	return slice1
}

func Intersect(slice1, slice2 []string) []string {
	m := make(map[string]int)
	nn := make([]string, 0)
	for _, v := range slice1 {
		m[v]++
	}

	for _, v := range slice2 {
		times, _ := m[v]
		if times == 1 {
			nn = append(nn, v)
		}
	}
	return nn
}


func Difference(a,b  []string) []string {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	itr := Intersect(a, b)
	m := map[string]int{}
	for _, v := range itr {
		m[v]++
	}
	var df []string
	for _, v := range a {
		if m[v] == 0 {
			df = append(df, v)
		}
	}
	for _, v := range b {
		if m[v] == 0 {
			df = append(df, v)
		}
	}
	return df
}

func ConvertoStringSlice(inter []interface{}) (res []string,err error)  {
	res = make([]string,0)
	for _,value := range inter{
		v,ok :=value.(string)
		if ok {
			res=append(res,v)
		}else {
			fmt.Printf("convert obj %+v failed\n",value)
			return res,err
		}
	}
	return res,nil
}

func IsExistInStrArray(value string,array []string) bool  {
	for _,v := range array{
		if v == value{
			return true
		}

	}
	return false
}

func IsExitsItem(value interface{},array interface{}) bool  {
	switch  reflect.TypeOf(array).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(array)
		for i:=0;i<s.Len();i++ {
			if reflect.DeepEqual(value,s.Index(i).Interface()){
				return true
			}
		}
	}
	return false
}