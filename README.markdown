# beanstalk

A native clojure [beanstalkd](http://kr.github.com/beanstalkd/) client library. 
Some inspiration and ideas were taken from [cl-beanstalk](https://github.com/antifuchs/cl-beanstalk/).
Refactored by bengl3rt

## Usage

    ; producer
	user=> (use 'beanstalk.core)
    user=> (def b (new-beanstalk))
    user=> (use b "my-tube")
    user=> (put b 0 0 0 5 "hello")
    ...
    ; consumer
	user=> (use 'beanstalk.core)
    user=> (def b (new-beanstalk))
    user=> (watch b "my-tube")
    user=> (def job (reserve b)) ; id is (:id job), payload is (:payload job)

## Installation

For clojure 1.2 projects:

    [org.clojars.sunng/beanstalk "1.0.5"]

For clojure 1.3 projects:

     [org.clojars.sunng/beanstalk "1.0.6"]

## Examples

Two examples are provided:

    ; start consumer
    lein run -m beanstalk.examples.consumer

    ; send some data
    lein run -m beanstalk.examples.producer -m "hello" -n 5
    ; send shutdown
    lein run -m beanstalk.examples.producer -m "exit" 

## License

Copyright (c) 2010 Damon Snyder 

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
