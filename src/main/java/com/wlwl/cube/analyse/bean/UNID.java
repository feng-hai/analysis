/**
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (c) 2007-2016 Ttron Kidman. All rights reserved.
 */
package com.wlwl.cube.analyse.bean;

import java.util.UUID;

/**
 * @Ttron 2015年8月10日
 */
public class UNID
{
	public static final String getUnid()
	{
		return getUnidLowerCase().toUpperCase();
	}


	public static final String getUnidLowerCase()
	{
		UUID uuid = UUID.randomUUID();
		String uuidString = uuid.toString();
		return uuidString.replaceAll( "-", "" );
	}
}
