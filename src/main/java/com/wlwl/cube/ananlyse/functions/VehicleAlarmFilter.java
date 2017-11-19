package com.wlwl.cube.ananlyse.functions;

import java.util.List;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

import com.wlwl.cube.analyse.bean.VehicleAlarmBean;

public class VehicleAlarmFilter extends BaseFilter {

	/**
	 * @Fields serialVersionUID : TODO(用一句话描述这个变量表示什么)
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public boolean isKeep(TridentTuple tuple) {
		// TODO Auto-generated method stub
		@SuppressWarnings("unchecked")
		List<VehicleAlarmBean> omok = (List<VehicleAlarmBean>) tuple.getValueByField("vehicleInfo");
		if (omok.size() > 0) {
			return false;
		} else {
			return true;
		}
	}

}
