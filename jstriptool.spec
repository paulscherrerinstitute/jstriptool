Summary:        Java Strip Tool
Name:           jstriptool
Version:        1.2.0
Release:        0%{?dist}
License:        GPL
Group:          Applications
BuildArch:      noarch
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-%(%{__id_u} -n)
Vendor:         PSI GFA Controls
Packager:       dockre@psi.ch
URL:            https://github.com/paulscherrerinstitute/jstriptool

Requires: java-11-openjdk

%define inst_folder                          /opt/%{name}
# %define git_folder                           %{_sourcedir}/%{name}-%{version}
# %define git_dir                              %{name}

%define debug_package                        %{nil}

%define _use_internal_dependency_generator   0
%define __find_provides                      %{nil}
%define __find_requires                      %{nil}

# Do not repack jar as this screws up permissions
# see https://stackoverflow.com/questions/31037967/rpmbuild-brp-java-repack-jars-changes-jar-permissions
%define __jar_repack %{nil}

%define _curdir %(echo $PWD)

%description
Java Epics Strip Tool

%prep
# %{_curdir}/gradlew clean
# %{_curdir}/gradlew build

%build

%install
%{__rm} -rf $RPM_BUILD_ROOT
%{__mkdir_p} $RPM_BUILD_ROOT%{inst_folder}/lib
%{__mkdir_p} $RPM_BUILD_ROOT/usr/local/bin
%{__cp} -a %{_curdir}/build/libs/jstriptool-*-fat.jar $RPM_BUILD_ROOT%{inst_folder}/lib/jstriptool-fat.jar
%{__cp} -a %{_curdir}/jstriptool $RPM_BUILD_ROOT/usr/local/bin
# ln -s jstriptool $RPM_BUILD_ROOT/usr/local/bin/
# %{__cp} -a %{_sourcedir}/build/libs/jstriptool-*-fat.jar $RPM_BUILD_ROOT%{inst_folder}/lib

%clean
%{__rm} -rf $RPM_BUILD_ROOT
# %{__rm} -rf %{git_folder}

%post
# ln -sf %{inst_folder}/lib/jstriptool-fat.jar /usr/local/bin/jstriptool

%preun
# if [ "$1" = "0" ] ; then # last uninstall
#   %{__rm} -f /usr/local/bin/jstriptool
# fi

%files
%attr(755,root,root) %{inst_folder}/lib/jstriptool-fat.jar
%defattr(-,root,root,0755)
/usr/local/bin/*