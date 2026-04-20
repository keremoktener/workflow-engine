package com.workflow.engine;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WorkflowContextTest {

    @Test
    void get_returnsEmpty_whenMissing() {
        assertTrue(WorkflowContext.empty().get("k", String.class).isEmpty());
    }

    @Test
    void get_throws_onTypeMismatch() {
        WorkflowContext ctx = WorkflowContext.empty().put("k", 123);
        assertThrows(ClassCastException.class, () -> ctx.get("k", String.class));
    }

    @Test
    void require_throws_whenMissing() {
        assertThrows(IllegalStateException.class,
            () -> WorkflowContext.empty().require("k", String.class));
    }
}
