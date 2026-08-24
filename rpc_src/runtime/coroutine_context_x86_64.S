.text
.globl coroutine_context_switch
.type coroutine_context_switch, @function
coroutine_context_switch:
    movq %rsi, %r9

    movq %rbx, 0(%rdi)
    movq %rbp, 8(%rdi)
    movq %r12, 16(%rdi)
    movq %r13, 24(%rdi)
    movq %r14, 32(%rdi)
    movq %r15, 40(%rdi)
    leaq 8(%rsp), %rax
    movq %rax, 48(%rdi)
    movq (%rsp), %rax
    movq %rax, 56(%rdi)

    testq %r8, %r8
    je .Lrestore_context
    movq %rdx, %rdi
    movq %rcx, %rsi
    movq %r8, %rcx
    cld
    rep movsb

.Lrestore_context:
    movq 0(%r9), %rbx
    movq 8(%r9), %rbp
    movq 16(%r9), %r12
    movq 24(%r9), %r13
    movq 32(%r9), %r14
    movq 40(%r9), %r15
    movq 48(%r9), %rsp
    pushq 56(%r9)
    ret
.size coroutine_context_switch, .-coroutine_context_switch

.globl coroutine_stack_pointer
.type coroutine_stack_pointer, @function
coroutine_stack_pointer:
    leaq 8(%rsp), %rax
    ret
.size coroutine_stack_pointer, .-coroutine_stack_pointer

.section .note.GNU-stack,"",@progbits
