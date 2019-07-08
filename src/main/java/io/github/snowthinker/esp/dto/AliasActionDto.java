package io.github.snowthinker.esp.dto;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

@Data
public class AliasActionDto<T extends Object> {

	private List<T> actions = new ArrayList<>();
	
}
