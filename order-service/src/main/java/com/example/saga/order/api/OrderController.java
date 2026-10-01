package com.example.saga.order.api;

import com.example.saga.order.domain.OrderRepository;
import com.example.saga.order.domain.OrderService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Size;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
@Validated
public class OrderController {

    private final OrderService orderService;
    private final OrderRepository orders;

    public OrderController(OrderService orderService, OrderRepository orders) {
        this.orderService = orderService;
        this.orders = orders;
    }

    /**
     * 202 Accepted: the order is PENDING until the saga finishes; poll the Location header for the outcome.
     * Clients should send an Idempotency-Key so that a retried POST does not start a second saga.
     */
    @PostMapping
    public ResponseEntity<OrderResponse> create(
            @Valid @RequestBody CreateOrderRequest request,
            @RequestHeader(name = "Idempotency-Key", required = false) @Size(max = 100) String idempotencyKey) {
        var order = orderService.placeOrder(request.productId(), request.quantity(), request.amount(), idempotencyKey);
        var location = ServletUriComponentsBuilder.fromCurrentRequest().path("/{id}").build(order.getId());
        return ResponseEntity.accepted().location(location).body(OrderResponse.from(order));
    }

    @GetMapping("/{id}")
    public ResponseEntity<OrderResponse> get(@PathVariable UUID id) {
        return ResponseEntity.of(orders.findById(id).map(OrderResponse::from));
    }

    @GetMapping
    public List<OrderResponse> list(@RequestParam(defaultValue = "50") int limit) {
        return orders.findAllByOrderByCreatedAtDesc(PageRequest.ofSize(Math.min(limit, 500)))
                .stream().map(OrderResponse::from).toList();
    }
}
