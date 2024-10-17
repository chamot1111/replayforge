function init() {
    print("hello from script");
}

function process(content, emit) {
  emit(content);
}

var count = 0;

function timer_handler(emit) {
  print("timer " + count);
  emit("timer " + count++);
}
