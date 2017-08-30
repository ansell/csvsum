package com.github.ansell.csv.sort;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class StringList extends ArrayList<String> implements List<String> {

	private static final long serialVersionUID = 2431094178469338878L;

	public StringList() {
		super();
	}

	public StringList(Collection<? extends String> c) {
		super(c);
	}

	public StringList(int initialCapacity) {
		super(initialCapacity);
	}
	
}
