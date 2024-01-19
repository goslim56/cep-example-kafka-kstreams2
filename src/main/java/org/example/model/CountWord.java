package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CountWord {
    private String word; // ex) "maple" | "랩업하는법" | "캐시" | "메이플",
    private Long count; // ex) "2020–08–28T09:20:26.187"
    private Long windowStartTimestamp; // ex) "2020–08–28T09:20:26.187"
    private Long windowEndTimestamp; // ex) "2020–08–28T09:20:26.187"
}