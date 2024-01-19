package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SearchWord  {
    private String word; // ex) "maple" | "랩업하는법" | "캐시" | "메이플",
    private String timestamp; // ex) "2020–08–28T09:20:26.187"
}